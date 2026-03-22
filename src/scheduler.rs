use crate::config::Config;
use crate::task_parser::{TaskFile, ComponentStatus};
#[cfg(feature = "kafka")]
use crate::kafka::producer::TaskProducer;
use crate::agent::AgentSpawner;
use crate::tracker::TaskTracker;
use std::path::Path;
use std::fs;
use tracing::{info, warn, error};
use fs2::FileExt;

pub struct Scheduler {
    config: Config,
    tracker: TaskTracker,
}

impl Scheduler {
    pub fn new(config: Config) -> Self {
        let tracker = TaskTracker::new(config.scheduler.task_queue_dir.clone());
        Self { config, tracker }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("pulsar-relay scheduler starting main loop");

        loop {
            if let Err(e) = self.tick().await {
                error!(error = %e, "scheduler tick failed");
            }

            info!(
                interval = self.config.scheduler.interval_secs,
                "sleeping until next tick"
            );
            tokio::time::sleep(
                std::time::Duration::from_secs(self.config.scheduler.interval_secs)
            ).await;
        }
    }

    async fn tick(&mut self) -> anyhow::Result<()> {
        //acquire lock - if another instance is running, skip
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.config.scheduler.lock_file)?;

        if lock_file.try_lock_exclusive().is_err() {
            info!("another scheduler instance is running, skipping tick");
            return Ok(());
        }

        info!("tick started — scanning for tasks");

        //load active tasks from tracker
        let active_tasks = self.tracker.list_active()?;

        for task_ref in &active_tasks {
            self.process_task(task_ref).await?;
        }

        //scan for new tasks in queue directory
        self.scan_queue().await?;

        //release lock (dropped when lock_file goes out of scope)
        drop(lock_file);

        info!(
            active_tasks = active_tasks.len(),
            "tick completed"
        );

        Ok(())
    }

    async fn scan_queue(&mut self) -> anyhow::Result<()> {
        let queue_dir = &self.config.scheduler.task_queue_dir;
        if !queue_dir.exists() {
            return Ok(());
        }

        let entries = fs::read_dir(queue_dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map(|e| e == "md").unwrap_or(false) {
                if let Err(e) = self.ingest_task(&path).await {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "failed to ingest task"
                    );
                }
            }
        }

        Ok(())
    }

    async fn ingest_task(&mut self, path: &Path) -> anyhow::Result<()> {
        let task = TaskFile::parse(path)?;

        if task.is_finished() {
            info!(task_id = %task.task_id, "task already finished, skipping");
            return Ok(());
        }

        if !self.tracker.is_tracked(&task.task_id) {
            info!(
                task_id = %task.task_id,
                components = task.components.len(),
                "ingesting new task"
            );
            self.tracker.track(task.clone())?;

            //produce component chunks to Kafka if enabled
            #[cfg(feature = "kafka")]
            if let Some(ref kafka_config) = self.config.kafka {
                if let Ok(producer) = TaskProducer::new(kafka_config) {
                    for component in &task.components {
                        if component.status == ComponentStatus::Pending {
                            if let Err(e) = producer.send_component(&task.task_id, component).await {
                                warn!(error = %e, "failed to enqueue component to kafka");
                            }
                        }
                    }
                } else {
                    warn!("kafka unavailable — operating in local-only mode");
                }
            }
        }

        //process the next pending component
        self.process_task_file(&task).await
    }

    async fn process_task(&mut self, task_ref: &str) -> anyhow::Result<()> {
        let task_path = self.tracker.get_task_path(task_ref)?;
        let task = TaskFile::parse(Path::new(&task_path))?;

        self.process_task_file(&task).await
    }

    async fn process_task_file(&mut self, task: &TaskFile) -> anyhow::Result<()> {
        //check if something is already in progress
        if task.has_in_progress() {
            info!(
                task_id = %task.task_id,
                "component still in progress, waiting"
            );
            return Ok(());
        }

        //find next pending component
        let component = match task.next_pending() {
            Some(c) => c,
            None => {
                if task.is_finished() {
                    info!(task_id = %task.task_id, "all components finished");
                }
                return Ok(());
            }
        };

        info!(
            task_id = %task.task_id,
            component = component.index,
            title = %component.title,
            agent = ?component.agent,
            "dispatching component to agent"
        );

        //mark component as in-progress
        TaskFile::update_component_status(
            Path::new(&task.path),
            component.index,
            ComponentStatus::InProgress,
            None,
        )?;

        //spawn agent to work on this component
        let agent_name = component.agent.as_deref()
            .unwrap_or("backend-developer");

        let spawner = AgentSpawner::new(&self.config.agent);
        match spawner.spawn(agent_name, &task.path, component.index).await {
            Ok(result) => {
                info!(
                    task_id = %task.task_id,
                    component = component.index,
                    "component completed successfully"
                );
                TaskFile::update_component_status(
                    Path::new(&task.path),
                    component.index,
                    ComponentStatus::Completed,
                    Some(&result),
                )?;
            }
            Err(e) => {
                error!(
                    task_id = %task.task_id,
                    component = component.index,
                    error = %e,
                    "component execution failed"
                );
                TaskFile::update_component_status(
                    Path::new(&task.path),
                    component.index,
                    ComponentStatus::Failed,
                    Some(&format!("Error: {}", e)),
                )?;
            }
        }

        //update tracker progress by re-parsing the file
        let updated_task = TaskFile::parse(Path::new(&task.path))?;
        let completed = updated_task.components.iter()
            .filter(|c| c.status == ComponentStatus::Completed || c.status == ComponentStatus::Failed)
            .count();
        self.tracker.update_progress(&task.task_id, completed)?;

        Ok(())
    }
}
