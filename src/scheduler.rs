use crate::agent::{PrepAgentSpawner, parse_status_line};
use crate::checksum;
use crate::config::Config;
use crate::task_parser::{TaskFile, Component, ComponentStatus, PlanKind, extract_plan_kind};
#[cfg(feature = "kafka")]
use crate::kafka::producer::TaskProducer;
#[cfg(feature = "kafka")]
use crate::kafka::consumer::ResultConsumer;
use crate::tracker::TaskTracker;
use std::path::{Path, PathBuf};
use std::collections::{HashMap, HashSet};
use std::fs;
use regex::Regex;
use tracing::{info, warn, error};
use fs2::FileExt;

pub struct Scheduler {
    config: Config,
    tracker: TaskTracker,
    active_plans: HashMap<String, ActivePlanState>,
    prep_spawner: PrepAgentSpawner,
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
    //timestamp when the current component was dispatched (for duration tracking)
    component_dispatched_at: Option<chrono::DateTime<chrono::Utc>>,
}

//find the plan filename for a given task_id from active plans map
fn find_plan_filename(active_plans: &HashMap<String, ActivePlanState>, task_id: &str) -> String {
    active_plans.iter()
        .find(|(_, state)| state.task_id == task_id)
        .map(|(filename, _)| filename.clone())
        .unwrap_or_else(|| "unknown".to_string())
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

//collect all .md file paths from a directory, recursing one level into subdirectories
//handles the case where executor places completed files in date subdirs (e.g. completed/2026-03-24/)
fn collect_md_files_recursive(dir: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return paths,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            //recurse one level into subdirectories (date dirs)
            if let Ok(sub_entries) = fs::read_dir(&path) {
                for sub_entry in sub_entries.flatten() {
                    let sub_path = sub_entry.path();
                    if sub_path.is_file() && sub_path.extension().map(|e| e == "md").unwrap_or(false) {
                        paths.push(sub_path);
                    }
                }
            }
        } else if path.is_file() && path.extension().map(|e| e == "md").unwrap_or(false) {
            paths.push(path);
        }
    }

    paths
}

//scan pipeline completed/ dir for finished tasks belonging to a plan
//returns vec of (component_index, result_text, execution_status) for completed pipeline tasks
//handles renamed files with _completed_TIMESTAMP suffix and date subdirectories
fn scan_pipeline_completed(
    completed_dir: &Path,
    task_id: &str,
) -> Vec<(usize, String, ComponentStatus)> {
    let sanitized_id = task_id.replace(' ', "_").replace('/', "_");
    let prefix = format!("task_pulsar_{}_", sanitized_id);
    let mut results = Vec::new();

    if !completed_dir.exists() {
        return results;
    }

    //regex to extract component index: after the prefix, capture digits before anything else
    //matches: task_pulsar_{id}_{INDEX}.md or task_pulsar_{id}_{INDEX}_completed_TIMESTAMP.md
    let pattern = format!(r"^task_pulsar_{}_(\d+)(?:_.*)?\.md$", regex::escape(&sanitized_id));
    let re = match Regex::new(&pattern) {
        Ok(r) => r,
        Err(_) => return results,
    };

    let md_files = collect_md_files_recursive(completed_dir);

    for path in &md_files {
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };

        if !name.starts_with(&prefix) {
            continue;
        }

        if let Some(caps) = re.captures(name) {
            if let Ok(idx) = caps[1].parse::<usize>() {
                //read the completed task file for any result content and status
                let content = fs::read_to_string(path).unwrap_or_default();
                let result = extract_completed_result(&content);
                let status = extract_execution_status(&content);

                info!(
                    file = %name,
                    task_id = %task_id,
                    component = idx,
                    status = %status,
                    "found completed pipeline task"
                );
                results.push((idx, result, status));
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

//extract execution status from a completed pipeline task file
//pipeline executors write **Execution Status:** COMPLETED|FAILED
//defaults to Completed if not found (backward compat)
fn extract_execution_status(content: &str) -> ComponentStatus {
    for line in content.lines() {
        if line.contains("**Execution Status:**") {
            let status = line.split("**Execution Status:**").nth(1).unwrap_or("").trim();
            if status.eq_ignore_ascii_case("FAILED") {
                return ComponentStatus::Failed;
            }
        }
    }
    ComponentStatus::Completed
}

//remove a completed pipeline task file after processing its result
//handles renamed files with _completed_TIMESTAMP suffix and date subdirectories
fn remove_completed_pipeline_task(completed_dir: &Path, task_id: &str, component_index: usize) {
    let sanitized_id = task_id.replace(' ', "_").replace('/', "_");

    //try exact filename first (fast path)
    let exact = format!("task_pulsar_{}_{}.md", sanitized_id, component_index);
    let exact_path = completed_dir.join(&exact);
    if exact_path.exists() {
        if let Err(e) = fs::remove_file(&exact_path) {
            warn!(path = %exact_path.display(), error = %e, "failed to remove completed pipeline task");
        } else {
            info!(path = %exact_path.display(), "removed processed completed pipeline task");
        }
        return;
    }

    //fallback: scan for renamed files (with _completed_TIMESTAMP suffix) and date subdirs
    let prefix = format!("task_pulsar_{}_{}", sanitized_id, component_index);
    let md_files = collect_md_files_recursive(completed_dir);

    for path in &md_files {
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };

        //match files starting with the component prefix (index followed by _ or .md)
        if name.starts_with(&prefix) && name.ends_with(".md") {
            //verify it's the right component (not a prefix collision, e.g. index 1 vs 10)
            let after_prefix = &name[prefix.len()..];
            if after_prefix.starts_with('.') || after_prefix.starts_with('_') {
                if let Err(e) = fs::remove_file(path) {
                    warn!(path = %path.display(), error = %e, "failed to remove completed pipeline task");
                } else {
                    info!(path = %path.display(), "removed processed completed pipeline task");
                }
                return;
            }
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

//--- Plan lifecycle transitions ---

#[derive(Debug, PartialEq)]
enum PlanOutcome {
    Completed,
    Failed,
}

//determine whether a finished plan completed successfully or had failures
fn plan_outcome(task: &TaskFile) -> PlanOutcome {
    if task.components.iter().any(|c| c.status == ComponentStatus::Failed) {
        PlanOutcome::Failed
    } else {
        PlanOutcome::Completed
    }
}

//generate timestamp suffix for plan file moves
//format: _2026-03-23T2145 (no seconds, avoids colons in filenames)
fn timestamp_suffix() -> String {
    chrono::Local::now().format("_%Y-%m-%dT%H%M").to_string()
}

//move a plan file from source to dest directory with timestamp suffix
//creates dest directory if needed, returns new path on success
fn move_plan_file(source: &Path, dest_dir: &Path, filename: &str) -> anyhow::Result<std::path::PathBuf> {
    fs::create_dir_all(dest_dir)?;
    let stem = filename.trim_end_matches(".md");
    let new_name = format!("{}{}.md", stem, timestamp_suffix());
    let dest = dest_dir.join(&new_name);
    fs::rename(source, &dest)?;
    Ok(dest)
}

//collect details about failed components for logging
//returns vec of (component_index, title, error_context)
fn failed_component_details(task: &TaskFile) -> Vec<(usize, String, Option<String>)> {
    task.components.iter()
        .filter(|c| c.status == ComponentStatus::Failed)
        .map(|c| (c.index, c.title.clone(), c.result.clone()))
        .collect()
}

impl Scheduler {
    pub fn new(config: Config) -> Self {
        let tracker = TaskTracker::new(config.scheduler.task_queue_dir.clone());
        let prep_spawner = PrepAgentSpawner::new(config.agent.clone());

        #[cfg(feature = "kafka")]
        let (kafka_producer, kafka_consumer) = init_kafka(&config);

        Self {
            config,
            tracker,
            active_plans: HashMap::new(),
            prep_spawner,
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

        //step 4: transition finished plans from active/ to completed/ or failed/
        self.transition_finished_plans();

        //step 5: scan queue dir for new tasks (backward compat)
        self.scan_queue().await?;

        //step 6: process all active tasks with checksum-based change detection
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

            for (component_index, result_text, exec_status) in &completed {
                let task_path = match self.tracker.get_task_path(task_id) {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                let path = Path::new(&task_path);

                //calculate duration from dispatch timestamp
                let plan_filename = find_plan_filename(&self.active_plans, task_id);
                let duration_secs = self.active_plans.values()
                    .find(|s| s.task_id == *task_id)
                    .and_then(|s| s.component_dispatched_at)
                    .map(|dispatched| {
                        let elapsed = chrono::Utc::now() - dispatched;
                        elapsed.num_seconds()
                    })
                    .unwrap_or(-1);

                info!(
                    event = "component.completed",
                    plan_filename = %plan_filename,
                    task_id = %task_id,
                    component = component_index,
                    duration_secs = duration_secs,
                    execution_status = %exec_status,
                    "component finished"
                );

                //telegram: component completed notification
                if let Some(ref tg) = self.config.telegram {
                    if tg.notify_component_completed {
                        if let Ok(task_for_notify) = TaskFile::parse(path) {
                            let total = task_for_notify.components.len();
                            let title = task_for_notify.components.iter()
                                .find(|c| c.index == *component_index)
                                .map(|c| c.title.as_str())
                                .unwrap_or("unknown");
                            let plan_name = plan_filename.trim_end_matches(".md");
                            let msg = format!(
                                "📦 `{}` — Component {}/{} done\n\n✓ {}",
                                plan_name, component_index, total, title
                            );
                            let cfg = tg.clone();
                            tokio::spawn(async move {
                                crate::telegram::notify(&cfg, &msg).await;
                            });
                        }
                    }
                }

                //reset dispatch timestamp for next component
                if let Some(state) = self.active_plans.values_mut().find(|s| s.task_id == *task_id) {
                    state.component_dispatched_at = None;
                }

                //mark component with execution status from pipeline
                //pipeline executors write **Execution Status:** COMPLETED|FAILED
                if let Err(e) = TaskFile::update_component_status(
                    path,
                    *component_index,
                    exec_status.clone(),
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

        let agent = component.agent.as_deref().unwrap_or("backend-developer");

        info!(
            task_id = %task.task_id,
            component = component.index,
            title = %component.title,
            agent = %agent,
            "dispatching component via prep agent"
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

        //2. extract plan kind (defaults to Execution if header missing — preserves
        //backward compat with plans written before the Plan Kind contract)
        let plan_content = fs::read_to_string(&task.path)?;
        let plan_kind = extract_plan_kind(&plan_content).unwrap_or(PlanKind::Execution);

        //3. spawn prep agent — agent writes the pipeline task file to ready/
        match self.prep_spawner.spawn(
            Path::new(&task.path),
            plan_kind,
            &self.config.scheduler.pipeline_ready_dir,
        ).await {
            Ok(stdout) => {
                let status = parse_status_line(&stdout).unwrap_or("ok unknown");
                info!(
                    task_id = %task.task_id,
                    component = component.index,
                    prep_status = %status,
                    "prep agent completed"
                );
            }
            Err(e) => {
                error!(
                    task_id = %task.task_id,
                    component = component.index,
                    error = %e,
                    "prep agent failed"
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
        }

        //3. publish to kafka for tracking/durability (non-fatal)
        self.publish_dispatch_to_kafka(task, component).await;

        //4. update tracker progress
        let updated_task = TaskFile::parse(Path::new(&task.path))?;
        let done = count_done_components(&updated_task);
        self.tracker.update_progress(&task.task_id, done)?;

        //5. record dispatch timestamp + emit structured event
        let plan_filename = find_plan_filename(&self.active_plans, &task.task_id);
        info!(
            event = "component.dispatched",
            plan_filename = %plan_filename,
            task_id = %task.task_id,
            component = component.index,
            agent = %agent,
            "component sent to pipeline"
        );

        //store dispatch timestamp for duration calculation on completion
        if let Some(state) = self.active_plans.values_mut().find(|s| s.task_id == task.task_id) {
            state.component_dispatched_at = Some(chrono::Utc::now());
            state.current_component = component.index;
        }

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

    //transition finished plans from active/ to completed/ or failed/
    //called after check_pipeline_completions and scan_active_plans
    //handles filesystem errors gracefully — logs and continues, never crashes
    fn transition_finished_plans(&mut self) {
        let plans_config = match self.config.plans.as_ref() {
            Some(p) => p.clone(),
            None => return,
        };

        //collect non-cancelled plan filenames to check
        let candidates: Vec<(String, String)> = self.active_plans.iter()
            .filter(|(_, state)| !state.cancelled)
            .map(|(filename, state)| (filename.clone(), state.task_id.clone()))
            .collect();

        for (filename, task_id) in candidates {
            let source = plans_config.active_dir.join(&filename);
            let task = match TaskFile::parse(&source) {
                Ok(t) => t,
                Err(e) => {
                    warn!(
                        filename = %filename,
                        error = %e,
                        "failed to parse plan for transition check"
                    );
                    continue;
                }
            };

            if !task.is_finished() {
                continue;
            }

            let outcome = plan_outcome(&task);
            let (dest_dir, _label) = match outcome {
                PlanOutcome::Completed => (&plans_config.completed_dir, "completed"),
                PlanOutcome::Failed => (&plans_config.failed_dir, "failed"),
            };

            match move_plan_file(&source, dest_dir, &filename) {
                Ok(new_path) => {
                    let component_count = task.components.len();

                    match outcome {
                        PlanOutcome::Completed => {
                            info!(
                                event = "plan.completed",
                                plan_filename = %filename,
                                task_id = %task_id,
                                component_count = component_count,
                                destination = %new_path.display(),
                                "all components done, moved to completed/"
                            );

                            //telegram: plan completed notification
                            if let Some(ref tg) = self.config.telegram {
                                if tg.notify_plan_completed {
                                    let component_list: Vec<String> = task.components.iter()
                                        .map(|c| format!("  ✓ C{}: {}", c.index, c.title))
                                        .collect();
                                    let msg = format!(
                                        "✅ *Pulsar Relay* — Plan completed\n\n📋 `{}`\n📊 {} components completed\n\n{}\n",
                                        filename.trim_end_matches(".md"), component_count, component_list.join("\n")
                                    );
                                    let cfg = tg.clone();
                                    tokio::spawn(async move {
                                        crate::telegram::notify(&cfg, &msg).await;
                                    });
                                }
                            }
                        }
                        PlanOutcome::Failed => {
                            let failures = failed_component_details(&task);
                            for (idx, title, result) in &failures {
                                error!(
                                    task_id = %task_id,
                                    plan_filename = %filename,
                                    failed_component = idx,
                                    component_title = %title,
                                    retry_count = 0,
                                    error_context = ?result,
                                    "component failed in plan"
                                );
                            }
                            info!(
                                event = "plan.failed",
                                plan_filename = %filename,
                                task_id = %task_id,
                                component_count = component_count,
                                failed_count = failures.len(),
                                destination = %new_path.display(),
                                "component failed, moved to failed/"
                            );

                            //telegram: plan failed notification
                            if let Some(ref tg) = self.config.telegram {
                                if tg.notify_plan_failed {
                                    let failed_summary: Vec<String> = failures.iter()
                                        .map(|(idx, title, result)| {
                                            let err = result.as_deref().unwrap_or("no details");
                                            format!("  💥 C{}: {} — {}", idx, title, err)
                                        })
                                        .collect();
                                    let msg = format!(
                                        "❌ *Pulsar Relay* — Plan failed\n\n📋 `{}`\n💀 {}/{} components failed:\n\n{}",
                                        filename.trim_end_matches(".md"), failures.len(), component_count, failed_summary.join("\n")
                                    );
                                    let cfg = tg.clone();
                                    tokio::spawn(async move {
                                        crate::telegram::notify(&cfg, &msg).await;
                                    });
                                }
                            }

                            //override tracker status to "failed"
                            if let Err(e) = self.tracker.mark_failed(&task_id) {
                                warn!(
                                    task_id = %task_id,
                                    error = %e,
                                    "failed to mark plan as failed in tracker"
                                );
                            }
                        }
                    }

                    //remove from active plans tracking
                    self.active_plans.remove(&filename);
                }
                Err(e) => {
                    //filesystem error — log and continue, don't crash the loop
                    error!(
                        task_id = %task_id,
                        filename = %filename,
                        dest_dir = %dest_dir.display(),
                        error = %e,
                        "failed to move plan file — will retry next tick"
                    );
                }
            }
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
                        let old_checksum = state.checksum.clone();
                        info!(
                            event = "plan.checksum_changed",
                            plan_filename = %filename,
                            task_id = %state.task_id,
                            old_checksum = %old_checksum,
                            new_checksum = %hash,
                            "plan modified mid-flight"
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

        info!(
            event = "plan.discovered",
            plan_filename = %filename,
            task_id = %task.task_id,
            component_count = total,
            current_component = cur_component,
            "new plan detected in active directory"
        );

        self.active_plans.insert(filename.to_string(), ActivePlanState {
            task_id: task.task_id,
            checksum: hash,
            current_component: cur_component,
            total_components: total,
            cancelled: false,
            component_dispatched_at: None,
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
                event = "plan.cancelled",
                plan_filename = %filename,
                task_id = %state.task_id,
                current_component = state.current_component,
                total_components = state.total_components,
                "plan removed from active/ while running"
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
        assert_eq!(results[0].2, ComponentStatus::Completed);
    }

    #[test]
    fn test_scan_pipeline_completed_detects_failed_status() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        fs::create_dir_all(&completed).unwrap();

        fs::write(
            completed.join("task_pulsar_task_test_001_1.md"),
            "# Failed\n**Result:** ERROR: Agent not found\n**Execution Status:** FAILED",
        ).unwrap();

        let results = scan_pipeline_completed(&completed, "task_test_001");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[0].2, ComponentStatus::Failed);
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
    fn test_extract_execution_status_completed() {
        let content = "# Task\n**Execution Status:** COMPLETED\n**Result:** OK";
        assert_eq!(extract_execution_status(content), ComponentStatus::Completed);
    }

    #[test]
    fn test_extract_execution_status_failed() {
        let content = "# Task\n**Result:** ERROR\n**Execution Status:** FAILED";
        assert_eq!(extract_execution_status(content), ComponentStatus::Failed);
    }

    #[test]
    fn test_extract_execution_status_default_completed() {
        let content = "# Task\n**Result:** No status line here";
        assert_eq!(extract_execution_status(content), ComponentStatus::Completed);
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
            component_dispatched_at: None,
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
            component_dispatched_at: None,
        });
        plans.insert("plan-b.md".to_string(), ActivePlanState {
            task_id: "task_b".to_string(),
            checksum: "hash_b".to_string(),
            current_component: 1,
            total_components: 3,
            cancelled: false,
            component_dispatched_at: None,
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
            component_dispatched_at: None,
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
            component_dispatched_at: None,
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

    //--- Plan lifecycle transition tests ---

    fn make_component(index: usize, status: ComponentStatus) -> crate::task_parser::Component {
        crate::task_parser::Component {
            index,
            title: format!("Component {}", index),
            status,
            agent: Some("test-agent".to_string()),
            content: "Work.".to_string(),
            result: None,
        }
    }

    fn make_component_with_result(
        index: usize,
        status: ComponentStatus,
        result: Option<&str>,
    ) -> crate::task_parser::Component {
        crate::task_parser::Component {
            index,
            title: format!("Component {}", index),
            status,
            agent: Some("test-agent".to_string()),
            content: "Work.".to_string(),
            result: result.map(|s| s.to_string()),
        }
    }

    fn make_task_with_components(components: Vec<crate::task_parser::Component>) -> TaskFile {
        TaskFile {
            path: String::new(),
            title: "Test Plan".to_string(),
            task_id: "task_test_lifecycle".to_string(),
            components,
            scheduler_initiated: true,
        }
    }

    #[test]
    fn test_plan_outcome_all_completed() {
        let task = make_task_with_components(vec![
            make_component(1, ComponentStatus::Completed),
            make_component(2, ComponentStatus::Completed),
            make_component(3, ComponentStatus::Completed),
        ]);
        assert_eq!(plan_outcome(&task), PlanOutcome::Completed);
    }

    #[test]
    fn test_plan_outcome_with_failure() {
        let task = make_task_with_components(vec![
            make_component(1, ComponentStatus::Completed),
            make_component(2, ComponentStatus::Failed),
            make_component(3, ComponentStatus::Completed),
        ]);
        assert_eq!(plan_outcome(&task), PlanOutcome::Failed);
    }

    #[test]
    fn test_plan_outcome_all_failed() {
        let task = make_task_with_components(vec![
            make_component(1, ComponentStatus::Failed),
            make_component(2, ComponentStatus::Failed),
        ]);
        assert_eq!(plan_outcome(&task), PlanOutcome::Failed);
    }

    #[test]
    fn test_timestamp_suffix_format() {
        let suffix = timestamp_suffix();
        //format: _YYYY-MM-DDTHHMM
        assert!(suffix.starts_with('_'));
        assert_eq!(suffix.len(), 16); //_2026-03-23T2145
        assert!(suffix.contains('T'));
        //no colons (filesystem-safe)
        assert!(!suffix.contains(':'));
    }

    #[test]
    fn test_move_plan_file_success() {
        let tmp = TempDir::new().unwrap();
        let active = tmp.path().join("active");
        let completed = tmp.path().join("completed");
        fs::create_dir_all(&active).unwrap();

        let source = active.join("my-plan.md");
        fs::write(&source, "# Plan content").unwrap();

        let result = move_plan_file(&source, &completed, "my-plan.md");
        assert!(result.is_ok());

        let new_path = result.unwrap();
        //source should be gone
        assert!(!source.exists());
        //dest should exist
        assert!(new_path.exists());
        //dest dir created
        assert!(completed.exists());
        //filename has timestamp suffix
        let name = new_path.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("my-plan_"));
        assert!(name.ends_with(".md"));
        assert!(name.contains('T'));
        //content preserved
        let content = fs::read_to_string(&new_path).unwrap();
        assert_eq!(content, "# Plan content");
    }

    #[test]
    fn test_move_plan_file_creates_dest_dir() {
        let tmp = TempDir::new().unwrap();
        let active = tmp.path().join("active");
        let deep_dest = tmp.path().join("a").join("b").join("completed");
        fs::create_dir_all(&active).unwrap();

        let source = active.join("plan.md");
        fs::write(&source, "content").unwrap();

        let result = move_plan_file(&source, &deep_dest, "plan.md");
        assert!(result.is_ok());
        assert!(deep_dest.exists());
    }

    #[test]
    fn test_move_plan_file_nonexistent_source() {
        let tmp = TempDir::new().unwrap();
        let source = tmp.path().join("active").join("ghost.md");
        let dest = tmp.path().join("completed");

        let result = move_plan_file(&source, &dest, "ghost.md");
        assert!(result.is_err());
    }

    #[test]
    fn test_failed_component_details_extracts_failures() {
        let task = make_task_with_components(vec![
            make_component(1, ComponentStatus::Completed),
            make_component_with_result(2, ComponentStatus::Failed, Some("permission denied")),
            make_component(3, ComponentStatus::Completed),
            make_component_with_result(4, ComponentStatus::Failed, None),
        ]);

        let failures = failed_component_details(&task);
        assert_eq!(failures.len(), 2);
        assert_eq!(failures[0].0, 2);
        assert_eq!(failures[0].1, "Component 2");
        assert_eq!(failures[0].2, Some("permission denied".to_string()));
        assert_eq!(failures[1].0, 4);
        assert_eq!(failures[1].2, None);
    }

    #[test]
    fn test_failed_component_details_no_failures() {
        let task = make_task_with_components(vec![
            make_component(1, ComponentStatus::Completed),
            make_component(2, ComponentStatus::Completed),
        ]);

        let failures = failed_component_details(&task);
        assert!(failures.is_empty());
    }

    //--- Logging & observability tests ---

    #[test]
    fn test_find_plan_filename_found() {
        let mut plans: HashMap<String, ActivePlanState> = HashMap::new();
        plans.insert("my-plan.md".to_string(), ActivePlanState {
            task_id: "task_my_plan_001".to_string(),
            checksum: "hash".to_string(),
            current_component: 1,
            total_components: 3,
            cancelled: false,
            component_dispatched_at: None,
        });

        assert_eq!(find_plan_filename(&plans, "task_my_plan_001"), "my-plan.md");
    }

    #[test]
    fn test_find_plan_filename_not_found() {
        let plans: HashMap<String, ActivePlanState> = HashMap::new();
        assert_eq!(find_plan_filename(&plans, "task_nonexistent"), "unknown");
    }

    #[test]
    fn test_find_plan_filename_multiple_plans() {
        let mut plans: HashMap<String, ActivePlanState> = HashMap::new();
        plans.insert("alpha.md".to_string(), ActivePlanState {
            task_id: "task_alpha".to_string(),
            checksum: "h1".to_string(),
            current_component: 1,
            total_components: 2,
            cancelled: false,
            component_dispatched_at: None,
        });
        plans.insert("beta.md".to_string(), ActivePlanState {
            task_id: "task_beta".to_string(),
            checksum: "h2".to_string(),
            current_component: 1,
            total_components: 3,
            cancelled: false,
            component_dispatched_at: None,
        });

        assert_eq!(find_plan_filename(&plans, "task_alpha"), "alpha.md");
        assert_eq!(find_plan_filename(&plans, "task_beta"), "beta.md");
    }

    #[test]
    fn test_dispatch_timestamp_tracking() {
        let mut state = ActivePlanState {
            task_id: "task_ts_001".to_string(),
            checksum: "hash".to_string(),
            current_component: 1,
            total_components: 3,
            cancelled: false,
            component_dispatched_at: None,
        };

        //initially no timestamp
        assert!(state.component_dispatched_at.is_none());

        //simulate dispatch
        let dispatch_time = chrono::Utc::now();
        state.component_dispatched_at = Some(dispatch_time);
        assert!(state.component_dispatched_at.is_some());

        //simulate completion — calculate duration
        let duration = chrono::Utc::now() - state.component_dispatched_at.unwrap();
        assert!(duration.num_seconds() >= 0);

        //reset after completion
        state.component_dispatched_at = None;
        assert!(state.component_dispatched_at.is_none());
    }

    //--- Bug fix tests: filename pattern matching with _completed_ suffix ---

    #[test]
    fn test_scan_pipeline_completed_with_completed_suffix() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        fs::create_dir_all(&completed).unwrap();

        //executor renames files with _completed_TIMESTAMP suffix
        fs::write(
            completed.join("task_pulsar_task_test_001_8_completed_20260324_002657.md"),
            "# Completed\n**Result:** All done\n**Execution Status:** COMPLETED",
        ).unwrap();

        let results = scan_pipeline_completed(&completed, "task_test_001");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 8);
        assert_eq!(results[0].1, "All done");
        assert_eq!(results[0].2, ComponentStatus::Completed);
    }

    #[test]
    fn test_scan_pipeline_completed_in_date_subdir() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        let date_dir = completed.join("2026-03-24");
        fs::create_dir_all(&date_dir).unwrap();

        //executor puts files in date subdirectories
        fs::write(
            date_dir.join("task_pulsar_task_test_001_3_completed_20260324_150000.md"),
            "# Completed\n**Result:** Done in subdir",
        ).unwrap();

        let results = scan_pipeline_completed(&completed, "task_test_001");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 3);
        assert_eq!(results[0].1, "Done in subdir");
    }

    #[test]
    fn test_scan_pipeline_completed_mixed_locations() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        let date_dir = completed.join("2026-03-24");
        fs::create_dir_all(&date_dir).unwrap();

        //one in flat dir (original naming)
        fs::write(
            completed.join("task_pulsar_task_test_001_1.md"),
            "# Completed\n**Result:** Flat",
        ).unwrap();

        //one in date subdir with _completed_ suffix
        fs::write(
            date_dir.join("task_pulsar_task_test_001_2_completed_20260324_150000.md"),
            "# Completed\n**Result:** Subdir",
        ).unwrap();

        let mut results = scan_pipeline_completed(&completed, "task_test_001");
        results.sort_by_key(|r| r.0);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[1].0, 2);
    }

    #[test]
    fn test_scan_pipeline_completed_no_index_collision() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        fs::create_dir_all(&completed).unwrap();

        //index 1 and index 10 should not collide
        fs::write(
            completed.join("task_pulsar_task_test_001_1_completed_20260324_120000.md"),
            "# One",
        ).unwrap();
        fs::write(
            completed.join("task_pulsar_task_test_001_10_completed_20260324_120001.md"),
            "# Ten",
        ).unwrap();

        let mut results = scan_pipeline_completed(&completed, "task_test_001");
        results.sort_by_key(|r| r.0);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[1].0, 10);
    }

    #[test]
    fn test_remove_completed_pipeline_task_with_suffix() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        fs::create_dir_all(&completed).unwrap();

        let file = completed.join("task_pulsar_task_test_001_5_completed_20260324_120000.md");
        fs::write(&file, "# done").unwrap();
        assert!(file.exists());

        remove_completed_pipeline_task(&completed, "task_test_001", 5);
        assert!(!file.exists());
    }

    #[test]
    fn test_remove_completed_pipeline_task_in_date_subdir() {
        let tmp = TempDir::new().unwrap();
        let completed = tmp.path().join("completed");
        let date_dir = completed.join("2026-03-24");
        fs::create_dir_all(&date_dir).unwrap();

        let file = date_dir.join("task_pulsar_task_test_001_2_completed_20260324_150000.md");
        fs::write(&file, "# done").unwrap();
        assert!(file.exists());

        remove_completed_pipeline_task(&completed, "task_test_001", 2);
        assert!(!file.exists());
    }

    #[test]
    fn test_collect_md_files_recursive_flat_and_subdirs() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("completed");
        let subdir = dir.join("2026-03-24");
        fs::create_dir_all(&subdir).unwrap();

        fs::write(dir.join("flat.md"), "# flat").unwrap();
        fs::write(dir.join("not-md.txt"), "skip").unwrap();
        fs::write(subdir.join("nested.md"), "# nested").unwrap();
        fs::write(subdir.join("also-skip.json"), "{}").unwrap();

        let files = collect_md_files_recursive(&dir);
        assert_eq!(files.len(), 2);
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
