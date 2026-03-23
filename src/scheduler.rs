use crate::checksum;
use crate::config::Config;
use crate::task_parser::{TaskFile, Component, ComponentStatus};
#[cfg(feature = "kafka")]
use crate::kafka::producer::TaskProducer;
#[cfg(feature = "kafka")]
use crate::kafka::consumer::ResultConsumer;
use crate::agent::AgentSpawner;
use crate::tracker::TaskTracker;
use std::path::Path;
use std::fs;
use tracing::{info, warn, error};
use fs2::FileExt;

pub struct Scheduler {
    config: Config,
    tracker: TaskTracker,
    #[cfg(feature = "kafka")]
    kafka_producer: Option<TaskProducer>,
    #[cfg(feature = "kafka")]
    kafka_consumer: Option<ResultConsumer>,
}

//write a pipeline task file for a dispatched component
fn write_pipeline_task(
    pipeline_ready_dir: &Path,
    task: &TaskFile,
    component: &Component,
) -> anyhow::Result<String> {
    fs::create_dir_all(pipeline_ready_dir)?;

    let sanitized_id = task.task_id.replace(' ', "_").replace('/', "_");
    let filename = format!("task_pulsar_{}_{}.md", sanitized_id, component.index);
    let agent = component.agent.as_deref().unwrap_or("backend-developer");

    let content = format!(
        r#"# Task: {} — Component {}

**Task ID:** {}_component_{}
**Created:** {}
**Scheduler:** pulsar-relay
**Plan File:** {}
**Component:** {}
**Priority:** Medium
**Type:** code
**Target Agent:** {}
**Agent Available:** Yes
**Routing Confidence:** 95%
**Ready Status:** READY_FOR_EXECUTION

## Summary
Component {} of plan "{}": {}

## Instructions

{}

## Dynamic Agent Config
```json
{{
  "agent": "{}",
  "source": "agents/models/default/{}.json"
}}
```
"#,
        task.title,
        component.index,
        sanitized_id,
        component.index,
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"),
        task.path,
        component.index,
        agent,
        component.index,
        task.title,
        component.title,
        component.content,
        agent,
        agent,
    );

    let filepath = pipeline_ready_dir.join(&filename);
    fs::write(&filepath, content)?;

    info!(
        file = %filepath.display(),
        task_id = %task.task_id,
        component = component.index,
        "pipeline task file written"
    );

    Ok(filename)
}

//count completed + failed components
fn count_done_components(task: &TaskFile) -> usize {
    task.components.iter()
        .filter(|c| c.status == ComponentStatus::Completed || c.status == ComponentStatus::Failed)
        .count()
}

//check if plan file changed since last known checksum
fn plan_file_changed(tracker: &TaskTracker, task_id: &str, plan_path: &Path) -> (bool, String) {
    match checksum::hash_file(plan_path) {
        Ok(current_hash) => {
            let stored = tracker.get_checksum(task_id);
            let changed = stored.as_deref() != Some(&current_hash);
            (changed, current_hash)
        }
        Err(e) => {
            warn!(task_id = %task_id, error = %e, "failed to hash plan file — treating as changed");
            (true, String::new())
        }
    }
}

impl Scheduler {
    pub fn new(config: Config) -> Self {
        let tracker = TaskTracker::new(config.scheduler.task_queue_dir.clone());

        #[cfg(feature = "kafka")]
        let (kafka_producer, kafka_consumer) = init_kafka(&config);

        Self {
            config,
            tracker,
            #[cfg(feature = "kafka")]
            kafka_producer,
            #[cfg(feature = "kafka")]
            kafka_consumer,
        }
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
        //acquire lock — if another instance is running, skip
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.config.scheduler.lock_file)?;

        if lock_file.try_lock_exclusive().is_err() {
            info!("another scheduler instance is running, skipping tick");
            return Ok(());
        }

        info!("tick started");

        //step 1: consume any completed results from kafka
        self.consume_kafka_results().await;

        //step 2: scan queue dir for new tasks (adds them to tracker)
        self.scan_queue().await?;

        //step 3: process all active tasks with checksum-based change detection
        let active_tasks = self.tracker.list_active()?;
        for task_ref in &active_tasks {
            if let Err(e) = self.process_task_with_checksum(task_ref).await {
                error!(task_id = %task_ref, error = %e, "failed to process task");
            }
        }

        //release lock
        drop(lock_file);

        info!(active_tasks = active_tasks.len(), "tick completed");
        Ok(())
    }

    //consume completed results from kafka and apply to plan files
    async fn consume_kafka_results(&mut self) {
        #[cfg(feature = "kafka")]
        {
            if let Some(ref consumer) = self.kafka_consumer {
                match consumer.drain_results().await {
                    Ok(results) => {
                        for result in results {
                            if let Err(e) = self.apply_kafka_result(
                                &result.task_id,
                                result.component_index,
                                &result.status,
                                result.output.as_deref(),
                            ) {
                                error!(
                                    task_id = %result.task_id,
                                    component = result.component_index,
                                    error = %e,
                                    "failed to apply kafka result"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to drain kafka results");
                    }
                }
            }
        }
    }

    //apply a single result from kafka to the plan file
    #[cfg(feature = "kafka")]
    fn apply_kafka_result(
        &mut self,
        task_id: &str,
        component_index: usize,
        status: &str,
        output: Option<&str>,
    ) -> anyhow::Result<()> {
        let task_path = self.tracker.get_task_path(task_id)?;
        let path = Path::new(&task_path);

        let new_status = match status.to_uppercase().as_str() {
            "COMPLETED" => ComponentStatus::Completed,
            "FAILED" => ComponentStatus::Failed,
            other => {
                warn!(status = %other, "unknown kafka result status, skipping");
                return Ok(());
            }
        };

        TaskFile::update_component_status(path, component_index, new_status, output)?;

        if let Err(e) = TaskFile::update_plan_header(path) {
            warn!(error = %e, "failed to update plan header after kafka result");
        }

        //update tracker progress
        let task = TaskFile::parse(path)?;
        let done = count_done_components(&task);
        self.tracker.update_progress(task_id, done)?;

        info!(
            task_id = %task_id,
            component = component_index,
            status = %status,
            "applied kafka result to plan"
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

    //ingest a new task: track it, optionally publish to kafka
    //does NOT dispatch — the main loop handles that via process_task_with_checksum
    async fn ingest_task(&mut self, path: &Path) -> anyhow::Result<()> {
        let task = TaskFile::parse(path)?;

        if task.is_finished() {
            return Ok(());
        }

        if self.tracker.is_tracked(&task.task_id) {
            return Ok(());
        }

        info!(
            task_id = %task.task_id,
            components = task.components.len(),
            "ingesting new task from queue"
        );
        self.tracker.track(task.clone())?;

        //publish pending components to kafka if available
        #[cfg(feature = "kafka")]
        {
            if let Some(ref producer) = self.kafka_producer {
                let hash = checksum::hash_file(path).unwrap_or_default();
                for component in &task.components {
                    if component.status == ComponentStatus::Pending {
                        if let Err(e) = producer.send_dispatch(
                            &task.task_id,
                            component,
                            &task.path,
                            &hash,
                        ).await {
                            warn!(error = %e, "failed to enqueue component to kafka");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    //process an active task with checksum-based change detection
    //skips if plan file hasn't changed since last tick
    async fn process_task_with_checksum(&mut self, task_ref: &str) -> anyhow::Result<()> {
        let task_path = self.tracker.get_task_path(task_ref)?;
        let path = Path::new(&task_path);

        if !path.exists() {
            warn!(task_id = %task_ref, path = %task_path, "plan file not found, skipping");
            return Ok(());
        }

        //compute current checksum and compare with stored
        let (changed, _current_hash) = plan_file_changed(&self.tracker, task_ref, path);

        if !changed {
            info!(task_id = %task_ref, "plan unchanged since last tick, skipping");
            return Ok(());
        }

        //plan is new (no stored hash) or changed — re-evaluate
        info!(task_id = %task_ref, "plan changed or new — evaluating components");

        let task = TaskFile::parse(path)?;
        self.dispatch_next_component(&task).await?;

        //store new checksum AFTER dispatch (dispatch modifies the plan file)
        if let Ok(new_hash) = checksum::hash_file(path) {
            self.tracker.set_checksum(task_ref, &new_hash)?;
        }

        Ok(())
    }

    //dispatch the next pending component via filesystem + kafka
    async fn dispatch_next_component(&mut self, task: &TaskFile) -> anyhow::Result<()> {
        //strict sequential: skip if any component is currently in progress
        if task.has_in_progress() {
            info!(task_id = %task.task_id, "component still in progress, waiting");
            return Ok(());
        }

        //find next pending component
        let component = match task.next_pending() {
            Some(c) => c,
            None => {
                if task.is_finished() {
                    info!(task_id = %task.task_id, "all components finished");
                    if let Err(e) = TaskFile::update_plan_header(Path::new(&task.path)) {
                        warn!(error = %e, "failed to update plan header");
                    }
                }
                return Ok(());
            }
        };

        info!(
            task_id = %task.task_id,
            component = component.index,
            title = %component.title,
            agent = ?component.agent,
            "dispatching component"
        );

        //1. write pipeline task file (filesystem dispatch)
        if let Err(e) = write_pipeline_task(
            &self.config.scheduler.pipeline_ready_dir,
            task,
            component,
        ) {
            warn!(error = %e, "failed to write pipeline task file (non-fatal)");
        }

        //2. publish to kafka (non-fatal — filesystem is fallback)
        self.publish_dispatch_to_kafka(task, component).await;

        //3. mark component as in-progress
        TaskFile::update_component_status(
            Path::new(&task.path),
            component.index,
            ComponentStatus::InProgress,
            None,
        )?;

        if let Err(e) = TaskFile::update_plan_header(Path::new(&task.path)) {
            warn!(error = %e, "failed to update plan header");
        }

        //4. spawn agent to work on this component
        let agent_name = component.agent.as_deref().unwrap_or("backend-developer");
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

        //5. update plan header and tracker
        if let Err(e) = TaskFile::update_plan_header(Path::new(&task.path)) {
            warn!(error = %e, "failed to update plan header");
        }

        let updated_task = TaskFile::parse(Path::new(&task.path))?;
        let done = count_done_components(&updated_task);
        self.tracker.update_progress(&task.task_id, done)?;

        Ok(())
    }

    //publish a component dispatch to kafka (non-fatal)
    async fn publish_dispatch_to_kafka(&self, task: &TaskFile, component: &Component) {
        #[cfg(feature = "kafka")]
        {
            if let Some(ref producer) = self.kafka_producer {
                let hash = checksum::hash_file(Path::new(&task.path)).unwrap_or_default();
                if let Err(e) = producer.send_dispatch(
                    &task.task_id,
                    component,
                    &task.path,
                    &hash,
                ).await {
                    warn!(
                        task_id = %task.task_id,
                        component = component.index,
                        error = %e,
                        "kafka dispatch failed — filesystem fallback active"
                    );
                }
            }
        }

        #[cfg(not(feature = "kafka"))]
        {
            let _ = (task, component);
        }
    }
}

//initialize kafka producer and consumer from config (non-fatal on failure)
#[cfg(feature = "kafka")]
fn init_kafka(config: &Config) -> (Option<TaskProducer>, Option<ResultConsumer>) {
    match config.kafka.as_ref() {
        Some(kafka_config) => {
            let producer = match TaskProducer::new(kafka_config) {
                Ok(p) => {
                    info!("kafka producer initialized");
                    Some(p)
                }
                Err(e) => {
                    warn!(error = %e, "kafka producer init failed — filesystem-only mode");
                    None
                }
            };

            let consumer = match ResultConsumer::new(kafka_config) {
                Ok(c) => {
                    info!("kafka result consumer initialized");
                    Some(c)
                }
                Err(e) => {
                    warn!(error = %e, "kafka consumer init failed — no result polling");
                    None
                }
            };

            (producer, consumer)
        }
        None => {
            info!("no kafka config — operating in filesystem-only mode");
            (None, None)
        }
    }
}
