use crate::checksum;
use crate::config::Config;
use crate::task_parser::{TaskFile, Component, ComponentStatus};
#[cfg(feature = "kafka")]
use crate::kafka::producer::TaskProducer;
#[cfg(feature = "kafka")]
use crate::kafka::consumer::ResultConsumer;
use crate::tracker::TaskTracker;
use std::path::Path;
use std::collections::{HashMap, HashSet};
use std::fs;
use tracing::{info, warn, error};
use fs2::FileExt;

pub struct Scheduler {
    config: Config,
    tracker: TaskTracker,
    active_plans: HashMap<String, ActivePlanState>,
    #[cfg(feature = "kafka")]
    kafka_producer: Option<TaskProducer>,
    #[cfg(feature = "kafka")]
    kafka_consumer: Option<ResultConsumer>,
}

//count completed + failed components
fn count_done_components(task: &TaskFile) -> usize {
    task.components.iter()
        .filter(|c| c.status == ComponentStatus::Completed || c.status == ComponentStatus::Failed)
        .count()
}

//check if any pipeline task files for this task exist in ready/ or processing/ dirs
//prevents dispatching next component while pipeline executor is still running previous one
fn has_in_flight_pipeline_tasks(
    ready_dir: &Path,
    processing_dir: &Path,
    task_id: &str,
) -> bool {
    let sanitized_id = task_id.replace(' ', "_").replace('/', "_");
    let prefix = format!("task_pulsar_{}_", sanitized_id);

    for dir in [ready_dir, processing_dir] {
        if dir.exists() {
            if let Ok(entries) = fs::read_dir(dir) {
                for entry in entries.flatten() {
                    if let Some(name) = entry.file_name().to_str() {
                        if name.starts_with(&prefix) {
                            info!(
                                dir = %dir.display(),
                                file = %name,
                                task_id = %task_id,
                                "found in-flight pipeline task"
                            );
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

//remove pipeline task files for a completed/failed component
fn cleanup_pipeline_task(
    ready_dir: &Path,
    processing_dir: &Path,
    task_id: &str,
    component_index: usize,
) {
    let sanitized_id = task_id.replace(' ', "_").replace('/', "_");
    let filename = format!("task_pulsar_{}_{}.md", sanitized_id, component_index);

    for dir in [ready_dir, processing_dir] {
        let path = dir.join(&filename);
        if path.exists() {
            if let Err(e) = fs::remove_file(&path) {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "failed to clean up pipeline task file"
                );
            } else {
                info!(path = %path.display(), "cleaned up pipeline task file");
            }
        }
    }
}

//in-memory state for a plan discovered in the active directory
#[derive(Debug, Clone)]
struct ActivePlanState {
    task_id: String,
    checksum: String,
    current_component: usize,
    total_components: usize,
    cancelled: bool,
}

//list all .md filenames in a directory
fn list_md_files(dir: &Path) -> Vec<String> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    entries.flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_str()?.to_string();
            if name.ends_with(".md") { Some(name) } else { None }
        })
        .collect()
}

//determine the current component index for a plan
//returns the index of the first pending or in-progress component, or 0 if all done
fn current_component_index(task: &TaskFile) -> usize {
    task.components.iter()
        .find(|c| c.status == ComponentStatus::InProgress || c.status == ComponentStatus::Pending)
        .map(|c| c.index)
        .unwrap_or(0)
}

//write a pipeline task file for a dispatched component
//this is the ONLY dispatch method — pipeline executors pick up from ready/
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
        "pipeline task file written to ready/"
    );

    Ok(filename)
}

//scan pipeline completed/ dir for finished tasks belonging to a plan
//returns vec of (component_index, result_text) for completed pipeline tasks
fn scan_pipeline_completed(
    completed_dir: &Path,
    task_id: &str,
) -> Vec<(usize, String)> {
    let sanitized_id = task_id.replace(' ', "_").replace('/', "_");
    let prefix = format!("task_pulsar_{}_", sanitized_id);
    let mut results = Vec::new();

    if !completed_dir.exists() {
        return results;
    }

    let entries = match fs::read_dir(completed_dir) {
        Ok(e) => e,
        Err(_) => return results,
    };

    for entry in entries.flatten() {
        if let Some(name) = entry.file_name().to_str() {
            if name.starts_with(&prefix) && name.ends_with(".md") {
                //extract component index from filename: task_pulsar_{id}_{index}.md
                let stripped = name.trim_start_matches(&prefix).trim_end_matches(".md");
                if let Ok(idx) = stripped.parse::<usize>() {
                    //read the completed task file for any result content
                    let content = fs::read_to_string(entry.path())
                        .unwrap_or_default();
                    let result = extract_completed_result(&content);
                    results.push((idx, result));

                    info!(
                        file = %name,
                        task_id = %task_id,
                        component = idx,
                        "found completed pipeline task"
                    );
                }
            }
        }
    }

    results
}

//extract result text from a completed pipeline task file
fn extract_completed_result(content: &str) -> String {
    //look for Result section or output section
    for line in content.lines() {
        if line.contains("**Result:**") {
            return line.split("**Result:**").nth(1).unwrap_or("").trim().to_string();
        }
    }
    //fallback: completed by pipeline executor
    "Completed by pipeline executor".to_string()
}

//remove a completed pipeline task file after processing its result
fn remove_completed_pipeline_task(completed_dir: &Path, task_id: &str, component_index: usize) {
    let sanitized_id = task_id.replace(' ', "_").replace('/', "_");
    let filename = format!("task_pulsar_{}_{}.md", sanitized_id, component_index);
    let path = completed_dir.join(&filename);
    if path.exists() {
        if let Err(e) = fs::remove_file(&path) {
            warn!(path = %path.display(), error = %e, "failed to remove completed pipeline task");
        } else {
            info!(path = %path.display(), "removed processed completed pipeline task");
        }
    }
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
            active_plans: HashMap::new(),
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

        //step 2: check pipeline completed/ for finished tasks
        self.check_pipeline_completions().await;

        //step 3: scan active plans directory for new/removed plans
        if let Err(e) = self.scan_active_plans().await {
            warn!(error = %e, "failed to scan active plans");
        }

        //step 4: scan queue dir for new tasks (backward compat)
        self.scan_queue().await?;

        //step 5: process all active tasks with checksum-based change detection
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

    //check pipeline completed/ directory for finished tasks
    //when a pipeline executor finishes, the task file lands in completed/
    //we pick it up, update the plan file, and clean up
    async fn check_pipeline_completions(&mut self) {
        let active_tasks = match self.tracker.list_active() {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "failed to list active tasks for completion check");
                return;
            }
        };

        for task_id in &active_tasks {
            let completed = scan_pipeline_completed(
                &self.config.scheduler.pipeline_completed_dir,
                task_id,
            );

            for (component_index, result_text) in &completed {
                let task_path = match self.tracker.get_task_path(task_id) {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                let path = Path::new(&task_path);

                info!(
                    task_id = %task_id,
                    component = component_index,
                    "pipeline completion detected — updating plan"
                );

                //mark component as completed in the plan file
                if let Err(e) = TaskFile::update_component_status(
                    path,
                    *component_index,
                    ComponentStatus::Completed,
                    Some(result_text),
                ) {
                    error!(
                        task_id = %task_id,
                        component = component_index,
                        error = %e,
                        "failed to update component status from pipeline completion"
                    );
                    continue;
                }

                if let Err(e) = TaskFile::update_plan_header(path) {
                    warn!(error = %e, "failed to update plan header after completion");
                }

                //update tracker progress
                if let Ok(updated) = TaskFile::parse(path) {
                    let done = count_done_components(&updated);
                    if let Err(e) = self.tracker.update_progress(task_id, done) {
                        warn!(error = %e, "failed to update tracker progress");
                    }
                }

                //clean up: remove from completed/ and any stale files in ready/processing/
                remove_completed_pipeline_task(
                    &self.config.scheduler.pipeline_completed_dir,
                    task_id,
                    *component_index,
                );
                cleanup_pipeline_task(
                    &self.config.scheduler.pipeline_ready_dir,
                    &self.config.scheduler.pipeline_processing_dir,
                    task_id,
                    *component_index,
                );
            }
        }
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

        //only cache checksum when there's nothing to dispatch on next tick:
        //- no pending components (all done), or
        //- a component is in progress (must wait for it to complete)
        //if there are still pending components and nothing in-progress,
        //DON'T cache — next tick needs to dispatch
        let updated = TaskFile::parse(path)?;
        let has_pending = updated.next_pending().is_some();
        let has_in_progress = updated.has_in_progress();

        if !has_pending || has_in_progress {
            if let Ok(new_hash) = checksum::hash_file(path) {
                self.tracker.set_checksum(task_ref, &new_hash)?;
            }
        }

        Ok(())
    }

    //dispatch the next pending component via pipeline — non-blocking
    //writes task file to ready/, pipeline executors handle execution
    //completion detected on next tick via check_pipeline_completions()
    async fn dispatch_next_component(&mut self, task: &TaskFile) -> anyhow::Result<()> {
        //strict sequential check 1: skip if any component is marked IN_PROGRESS in plan file
        if task.has_in_progress() {
            info!(task_id = %task.task_id, "component still in progress (plan file), waiting");
            return Ok(());
        }

        //strict sequential check 2: skip if pipeline has in-flight tasks for this plan
        if has_in_flight_pipeline_tasks(
            &self.config.scheduler.pipeline_ready_dir,
            &self.config.scheduler.pipeline_processing_dir,
            &task.task_id,
        ) {
            info!(task_id = %task.task_id, "pipeline task still in flight (ready/ or processing/), waiting");
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
            "dispatching component to pipeline"
        );

        //1. mark IN_PROGRESS FIRST — before any dispatch (closes race window)
        TaskFile::update_component_status(
            Path::new(&task.path),
            component.index,
            ComponentStatus::InProgress,
            None,
        )?;

        if let Err(e) = TaskFile::update_plan_header(Path::new(&task.path)) {
            warn!(error = %e, "failed to update plan header");
        }

        //2. write pipeline task file to ready/ — pipeline executors pick this up
        if let Err(e) = write_pipeline_task(
            &self.config.scheduler.pipeline_ready_dir,
            task,
            component,
        ) {
            error!(
                task_id = %task.task_id,
                component = component.index,
                error = %e,
                "failed to write pipeline task file"
            );
            //rollback: mark component back to PENDING since dispatch failed
            TaskFile::update_component_status(
                Path::new(&task.path),
                component.index,
                ComponentStatus::Pending,
                None,
            )?;
            return Err(e);
        }

        //3. publish to kafka for tracking/durability (non-fatal)
        self.publish_dispatch_to_kafka(task, component).await;

        //4. update tracker progress
        let updated_task = TaskFile::parse(Path::new(&task.path))?;
        let done = count_done_components(&updated_task);
        self.tracker.update_progress(&task.task_id, done)?;

        info!(
            task_id = %task.task_id,
            component = component.index,
            "component dispatched to pipeline — executor will handle execution"
        );

        //DO NOT spawn agent — pipeline executor handles that
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

    //scan plans/active/ directory for plan files
    //discovers new plans, detects removed plans, tracks state in HashMap
    //backward compat: if PULSAR_PLAN_FILE env var is set, use single-plan mode
    async fn scan_active_plans(&mut self) -> anyhow::Result<()> {
        //backward compat: single-plan mode via env var
        if let Ok(plan_file) = std::env::var("PULSAR_PLAN_FILE") {
            if !plan_file.is_empty() {
                return self.ingest_single_plan_file(Path::new(&plan_file)).await;
            }
        }

        let active_dir = match self.config.plans.as_ref() {
            Some(p) => p.active_dir.clone(),
            None => return Ok(()),
        };

        if !active_dir.exists() {
            return Ok(());
        }

        let current_files: HashSet<String> = list_md_files(&active_dir)
            .into_iter()
            .collect();

        //detect new or reappeared files
        for filename in &current_files {
            let needs_register = match self.active_plans.get(filename.as_str()) {
                None => true,
                Some(s) if s.cancelled => true,
                Some(_) => false,
            };
            if needs_register {
                self.active_plans.remove(filename.as_str());
                let path = active_dir.join(filename);
                if let Err(e) = self.register_active_plan(filename, &path).await {
                    warn!(filename = %filename, error = %e, "failed to register active plan");
                }
            }
        }

        //detect removed files (in HashMap but no longer in directory)
        let tracked: Vec<String> = self.active_plans.keys().cloned().collect();
        for filename in tracked {
            if !current_files.contains(&filename) {
                self.cancel_active_plan(&filename);
            }
        }

        //update state for existing tracked plans (checksum + current component)
        for filename in &current_files {
            if let Some(state) = self.active_plans.get_mut(filename.as_str()) {
                if state.cancelled {
                    continue;
                }
                let path = active_dir.join(filename);
                if let Ok(hash) = checksum::hash_file(&path) {
                    if hash != state.checksum {
                        info!(
                            filename = %filename,
                            task_id = %state.task_id,
                            "active plan checksum changed"
                        );
                        state.checksum = hash;
                        if let Ok(task) = TaskFile::parse(&path) {
                            state.current_component = current_component_index(&task);
                            state.total_components = task.components.len();
                        }
                    }
                }
            }
        }

        Ok(())
    }

    //register a new plan discovered in the active directory
    async fn register_active_plan(&mut self, filename: &str, path: &Path) -> anyhow::Result<()> {
        let task = TaskFile::parse(path)?;

        if task.is_finished() {
            info!(filename = %filename, task_id = %task.task_id, "skipping finished plan in active/");
            return Ok(());
        }

        let hash = checksum::hash_file(path).unwrap_or_default();

        if self.tracker.is_tracked(&task.task_id) {
            //reactivation: plan came back after being cancelled or already known
            self.tracker.reactivate(&task.task_id)?;
            info!(filename = %filename, task_id = %task.task_id, "reactivated plan in active/");
        } else {
            //new plan: ingest via existing mechanism (tracker + kafka)
            self.ingest_task(path).await?;
        }

        let cur_component = current_component_index(&task);
        let total = task.components.len();

        self.active_plans.insert(filename.to_string(), ActivePlanState {
            task_id: task.task_id,
            checksum: hash,
            current_component: cur_component,
            total_components: total,
            cancelled: false,
        });

        Ok(())
    }

    //mark a plan as cancelled when its file disappears from active/
    fn cancel_active_plan(&mut self, filename: &str) {
        if let Some(state) = self.active_plans.get_mut(filename) {
            if state.cancelled {
                return;
            }
            info!(
                filename = %filename,
                task_id = %state.task_id,
                "plan removed from active/ — marking cancelled"
            );
            state.cancelled = true;
            if let Err(e) = self.tracker.mark_cancelled(&state.task_id) {
                warn!(task_id = %state.task_id, error = %e, "failed to mark cancelled in tracker");
            }
        }
    }

    //single-plan mode: ingest a specific plan file (backward compat with PULSAR_PLAN_FILE)
    async fn ingest_single_plan_file(&mut self, path: &Path) -> anyhow::Result<()> {
        if !path.exists() {
            warn!(path = %path.display(), "PULSAR_PLAN_FILE path does not exist");
            return Ok(());
        }
        self.ingest_task(path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_has_in_flight_pipeline_tasks_empty_dirs() {
        let tmp = TempDir::new().unwrap();
        let ready = tmp.path().join("ready");
        let processing = tmp.path().join("processing");
        fs::create_dir_all(&ready).unwrap();
        fs::create_dir_all(&processing).unwrap();

        assert!(!has_in_flight_pipeline_tasks(&ready, &processing, "task_test_001"));
    }

    #[test]
    fn test_has_in_flight_pipeline_tasks_finds_ready() {
        let tmp = TempDir::new().unwrap();
        let ready = tmp.path().join("ready");
        let processing = tmp.path().join("processing");
        fs::create_dir_all(&ready).unwrap();
        fs::create_dir_all(&processing).unwrap();

        //place a pipeline task in ready/
        fs::write(
            ready.join("task_pulsar_task_test_001_1.md"),
            "# pipeline task",
        ).unwrap();

        assert!(has_in_flight_pipeline_tasks(&ready, &processing, "task_test_001"));
    }

    #[test]
    fn test_has_in_flight_pipeline_tasks_finds_processing() {
        let tmp = TempDir::new().unwrap();
        let ready = tmp.path().join("ready");
        let processing = tmp.path().join("processing");
        fs::create_dir_all(&ready).unwrap();
        fs::create_dir_all(&processing).unwrap();

        //place a pipeline task in processing/
        fs::write(
            processing.join("task_pulsar_task_test_001_2.md"),
            "# pipeline task",
        ).unwrap();

        assert!(has_in_flight_pipeline_tasks(&ready, &processing, "task_test_001"));
    }

    #[test]
    fn test_has_in_flight_pipeline_tasks_ignores_other_tasks() {
        let tmp = TempDir::new().unwrap();
        let ready = tmp.path().join("ready");
        let processing = tmp.path().join("processing");
        fs::create_dir_all(&ready).unwrap();
        fs::create_dir_all(&processing).unwrap();

        //place a pipeline task for a DIFFERENT task
        fs::write(
            ready.join("task_pulsar_task_other_999_1.md"),
            "# other task",
        ).unwrap();

        assert!(!has_in_flight_pipeline_tasks(&ready, &processing, "task_test_001"));
    }

    #[test]
    fn test_has_in_flight_pipeline_tasks_nonexistent_dirs() {
        let tmp = TempDir::new().unwrap();
        let ready = tmp.path().join("nonexistent_ready");
        let processing = tmp.path().join("nonexistent_processing");

        //should not panic on nonexistent dirs
        assert!(!has_in_flight_pipeline_tasks(&ready, &processing, "task_test_001"));
    }

    #[test]
    fn test_cleanup_pipeline_task_removes_file() {
        let tmp = TempDir::new().unwrap();
        let ready = tmp.path().join("ready");
        let processing = tmp.path().join("processing");
        fs::create_dir_all(&ready).unwrap();
        fs::create_dir_all(&processing).unwrap();

        let ready_file = ready.join("task_pulsar_task_test_001_1.md");
        fs::write(&ready_file, "# task").unwrap();
        assert!(ready_file.exists());

        cleanup_pipeline_task(&ready, &processing, "task_test_001", 1);
        assert!(!ready_file.exists());
    }

    #[test]
    fn test_cleanup_pipeline_task_removes_from_processing() {
        let tmp = TempDir::new().unwrap();
        let ready = tmp.path().join("ready");
        let processing = tmp.path().join("processing");
        fs::create_dir_all(&ready).unwrap();
        fs::create_dir_all(&processing).unwrap();

        let proc_file = processing.join("task_pulsar_task_test_001_2.md");
        fs::write(&proc_file, "# task").unwrap();
        assert!(proc_file.exists());

        cleanup_pipeline_task(&ready, &processing, "task_test_001", 2);
        assert!(!proc_file.exists());
    }

    #[test]
    fn test_cleanup_pipeline_task_noop_when_no_file() {
        let tmp = TempDir::new().unwrap();
        let ready = tmp.path().join("ready");
        let processing = tmp.path().join("processing");
        fs::create_dir_all(&ready).unwrap();
        fs::create_dir_all(&processing).unwrap();

        //should not panic
        cleanup_pipeline_task(&ready, &processing, "task_test_001", 3);
    }

    #[test]
    fn test_write_pipeline_task_creates_file() {
        let tmp = TempDir::new().unwrap();
        let ready_dir = tmp.path().join("ready");

        let task = crate::task_parser::TaskFile {
            path: "/tmp/test-plan.md".to_string(),
            title: "Test Plan".to_string(),
            task_id: "task_test_write_001".to_string(),
            components: vec![crate::task_parser::Component {
                index: 1,
                title: "First step".to_string(),
                status: crate::task_parser::ComponentStatus::Pending,
                agent: Some("rust-developer".to_string()),
                content: "Do the thing.".to_string(),
                result: None,
            }],
            scheduler_initiated: true,
        };

        let filename = write_pipeline_task(&ready_dir, &task, &task.components[0]).unwrap();
        assert_eq!(filename, "task_pulsar_task_test_write_001_1.md");

        let filepath = ready_dir.join(&filename);
        assert!(filepath.exists());

        let content = fs::read_to_string(&filepath).unwrap();
        assert!(content.contains("**Scheduler:** pulsar-relay"));
        assert!(content.contains("**Target Agent:** rust-developer"));
        assert!(content.contains("**Plan File:** /tmp/test-plan.md"));
        assert!(content.contains("**Component:** 1"));
        assert!(content.contains("Do the thing."));
    }

    #[test]
    fn test_write_pipeline_task_default_agent() {
        let tmp = TempDir::new().unwrap();
        let ready_dir = tmp.path().join("ready");

        let task = crate::task_parser::TaskFile {
            path: "/tmp/plan.md".to_string(),
            title: "Plan".to_string(),
            task_id: "task_default_agent".to_string(),
            components: vec![crate::task_parser::Component {
                index: 1,
                title: "Step".to_string(),
                status: crate::task_parser::ComponentStatus::Pending,
                agent: None,
                content: "Work.".to_string(),
                result: None,
            }],
            scheduler_initiated: true,
        };

        write_pipeline_task(&ready_dir, &task, &task.components[0]).unwrap();
        let content = fs::read_to_string(ready_dir.join("task_pulsar_task_default_agent_1.md")).unwrap();
        assert!(content.contains("**Target Agent:** backend-developer"));
    }

    #[test]
    fn test_scan_pipeline_completed_empty() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        fs::create_dir_all(&completed).unwrap();

        let results = scan_pipeline_completed(&completed, "task_test_001");
        assert!(results.is_empty());
    }

    #[test]
    fn test_scan_pipeline_completed_finds_matching() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        fs::create_dir_all(&completed).unwrap();

        fs::write(
            completed.join("task_pulsar_task_test_001_1.md"),
            "# Completed\n**Result:** Schema created",
        ).unwrap();

        let results = scan_pipeline_completed(&completed, "task_test_001");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[0].1, "Schema created");
    }

    #[test]
    fn test_scan_pipeline_completed_ignores_other_tasks() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        fs::create_dir_all(&completed).unwrap();

        fs::write(
            completed.join("task_pulsar_task_other_999_1.md"),
            "# Other task",
        ).unwrap();

        let results = scan_pipeline_completed(&completed, "task_test_001");
        assert!(results.is_empty());
    }

    #[test]
    fn test_scan_pipeline_completed_nonexistent_dir() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("nonexistent");

        let results = scan_pipeline_completed(&completed, "task_test_001");
        assert!(results.is_empty());
    }

    #[test]
    fn test_extract_completed_result_with_result_line() {
        let content = "# Task\n**Result:** All tables created successfully\n## Done";
        assert_eq!(extract_completed_result(content), "All tables created successfully");
    }

    #[test]
    fn test_extract_completed_result_fallback() {
        let content = "# Task\nNo result line here";
        assert_eq!(extract_completed_result(content), "Completed by pipeline executor");
    }

    #[test]
    fn test_remove_completed_pipeline_task() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        fs::create_dir_all(&completed).unwrap();

        let file = completed.join("task_pulsar_task_test_001_1.md");
        fs::write(&file, "# done").unwrap();
        assert!(file.exists());

        remove_completed_pipeline_task(&completed, "task_test_001", 1);
        assert!(!file.exists());
    }

    #[test]
    fn test_remove_completed_pipeline_task_noop() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        fs::create_dir_all(&completed).unwrap();

        //should not panic
        remove_completed_pipeline_task(&completed, "task_test_001", 99);
    }

    #[test]
    fn test_list_md_files_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let files = list_md_files(tmp.path());
        assert!(files.is_empty());
    }

    #[test]
    fn test_list_md_files_finds_md_only() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("plan-a.md"), "# Plan A").unwrap();
        fs::write(tmp.path().join("plan-b.md"), "# Plan B").unwrap();
        fs::write(tmp.path().join("readme.txt"), "not a plan").unwrap();
        fs::write(tmp.path().join("data.json"), "{}").unwrap();

        let mut files = list_md_files(tmp.path());
        files.sort();
        assert_eq!(files, vec!["plan-a.md", "plan-b.md"]);
    }

    #[test]
    fn test_list_md_files_nonexistent_dir() {
        let files = list_md_files(Path::new("/tmp/nonexistent_pulsar_test_dir"));
        assert!(files.is_empty());
    }

    #[test]
    fn test_current_component_index_all_pending() {
        let task = crate::task_parser::TaskFile {
            path: String::new(),
            title: String::new(),
            task_id: String::new(),
            components: vec![
                crate::task_parser::Component {
                    index: 1, title: String::new(),
                    status: ComponentStatus::Pending,
                    agent: None, content: String::new(), result: None,
                },
                crate::task_parser::Component {
                    index: 2, title: String::new(),
                    status: ComponentStatus::Pending,
                    agent: None, content: String::new(), result: None,
                },
            ],
            scheduler_initiated: true,
        };
        assert_eq!(current_component_index(&task), 1);
    }

    #[test]
    fn test_current_component_index_mid_progress() {
        let task = crate::task_parser::TaskFile {
            path: String::new(),
            title: String::new(),
            task_id: String::new(),
            components: vec![
                crate::task_parser::Component {
                    index: 1, title: String::new(),
                    status: ComponentStatus::Completed,
                    agent: None, content: String::new(), result: None,
                },
                crate::task_parser::Component {
                    index: 2, title: String::new(),
                    status: ComponentStatus::InProgress,
                    agent: None, content: String::new(), result: None,
                },
                crate::task_parser::Component {
                    index: 3, title: String::new(),
                    status: ComponentStatus::Pending,
                    agent: None, content: String::new(), result: None,
                },
            ],
            scheduler_initiated: true,
        };
        assert_eq!(current_component_index(&task), 2);
    }

    #[test]
    fn test_current_component_index_all_done() {
        let task = crate::task_parser::TaskFile {
            path: String::new(),
            title: String::new(),
            task_id: String::new(),
            components: vec![
                crate::task_parser::Component {
                    index: 1, title: String::new(),
                    status: ComponentStatus::Completed,
                    agent: None, content: String::new(), result: None,
                },
                crate::task_parser::Component {
                    index: 2, title: String::new(),
                    status: ComponentStatus::Failed,
                    agent: None, content: String::new(), result: None,
                },
            ],
            scheduler_initiated: true,
        };
        assert_eq!(current_component_index(&task), 0);
    }

    #[test]
    fn test_active_plan_state_tracking() {
        let mut plans: HashMap<String, ActivePlanState> = HashMap::new();
        plans.insert("plan-a.md".to_string(), ActivePlanState {
            task_id: "task_a_001".to_string(),
            checksum: "abc123".to_string(),
            current_component: 1,
            total_components: 3,
            cancelled: false,
        });

        assert!(!plans["plan-a.md"].cancelled);
        assert_eq!(plans["plan-a.md"].current_component, 1);

        //cancel
        plans.get_mut("plan-a.md").unwrap().cancelled = true;
        assert!(plans["plan-a.md"].cancelled);

        //detect new vs known
        assert!(plans.contains_key("plan-a.md"));
        assert!(!plans.contains_key("plan-b.md"));
    }

    #[test]
    fn test_active_plan_state_multiple_plans() {
        let mut plans: HashMap<String, ActivePlanState> = HashMap::new();
        plans.insert("plan-a.md".to_string(), ActivePlanState {
            task_id: "task_a".to_string(),
            checksum: "hash_a".to_string(),
            current_component: 2,
            total_components: 5,
            cancelled: false,
        });
        plans.insert("plan-b.md".to_string(), ActivePlanState {
            task_id: "task_b".to_string(),
            checksum: "hash_b".to_string(),
            current_component: 1,
            total_components: 3,
            cancelled: false,
        });

        assert_eq!(plans.len(), 2);
        assert_eq!(plans["plan-a.md"].task_id, "task_a");
        assert_eq!(plans["plan-b.md"].task_id, "task_b");

        //cancel one, other remains active
        plans.get_mut("plan-a.md").unwrap().cancelled = true;
        let active_count = plans.values().filter(|s| !s.cancelled).count();
        assert_eq!(active_count, 1);
    }

    #[test]
    fn test_active_plan_state_checksum_update() {
        let mut plans: HashMap<String, ActivePlanState> = HashMap::new();
        plans.insert("plan.md".to_string(), ActivePlanState {
            task_id: "task_001".to_string(),
            checksum: "old_hash".to_string(),
            current_component: 1,
            total_components: 3,
            cancelled: false,
        });

        //simulate checksum change detection
        let new_hash = "new_hash";
        let state = plans.get_mut("plan.md").unwrap();
        assert_ne!(state.checksum, new_hash);
        state.checksum = new_hash.to_string();
        state.current_component = 2;
        assert_eq!(plans["plan.md"].checksum, "new_hash");
        assert_eq!(plans["plan.md"].current_component, 2);
    }

    #[test]
    fn test_detect_new_and_removed_plans() {
        let mut plans: HashMap<String, ActivePlanState> = HashMap::new();
        plans.insert("existing.md".to_string(), ActivePlanState {
            task_id: "task_existing".to_string(),
            checksum: "hash".to_string(),
            current_component: 1,
            total_components: 2,
            cancelled: false,
        });

        let current_files: HashSet<String> = ["existing.md", "new-plan.md"]
            .iter().map(|s| s.to_string()).collect();

        //detect new files
        let new_files: Vec<&String> = current_files.iter()
            .filter(|f| !plans.contains_key(f.as_str()))
            .collect();
        assert_eq!(new_files.len(), 1);
        assert_eq!(new_files[0], "new-plan.md");

        //detect removed files
        let removed: Vec<String> = plans.keys()
            .filter(|k| !current_files.contains(k.as_str()))
            .cloned()
            .collect();
        assert!(removed.is_empty());

        //now simulate removal
        let current_files2: HashSet<String> = ["new-plan.md"]
            .iter().map(|s| s.to_string()).collect();

        let removed2: Vec<String> = plans.keys()
            .filter(|k| !current_files2.contains(k.as_str()))
            .cloned()
            .collect();
        assert_eq!(removed2, vec!["existing.md"]);
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
