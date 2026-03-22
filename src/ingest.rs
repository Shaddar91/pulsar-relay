use crate::config::Config;
use crate::task_parser::TaskFile;
use crate::tracker::TaskTracker;
use std::path::Path;
use tracing::{info, warn};

//ingest a plan file or folder of plans into the scheduler
pub fn ingest_path(
    path: &Path,
    config: &Config,
    tracker: &mut TaskTracker,
) -> anyhow::Result<Vec<String>> {
    if path.is_file() {
        let id = ingest_file(path, tracker)?;
        Ok(vec![id])
    } else if path.is_dir() {
        ingest_folder(path, config, tracker)
    } else {
        Err(anyhow::anyhow!("path does not exist: {}", path.display()))
    }
}

//ingest a single plan file — validate and register with tracker
fn ingest_file(path: &Path, tracker: &mut TaskTracker) -> anyhow::Result<String> {
    let abs_path = std::fs::canonicalize(path)
        .map_err(|e| anyhow::anyhow!("cannot resolve path {}: {}", path.display(), e))?;

    let task = TaskFile::parse(&abs_path)?;

    if task.components.is_empty() {
        return Err(anyhow::anyhow!(
            "no components found in {} — expected ### Component N: or ### Phase N: headers",
            path.display()
        ));
    }

    if task.is_finished() {
        warn!(
            task_id = %task.task_id,
            path = %abs_path.display(),
            "plan already finished, skipping ingestion"
        );
        return Ok(task.task_id);
    }

    if tracker.is_tracked(&task.task_id) {
        info!(
            task_id = %task.task_id,
            "plan already tracked, skipping"
        );
        return Ok(task.task_id);
    }

    info!(
        task_id = %task.task_id,
        components = task.components.len(),
        path = %abs_path.display(),
        "ingesting plan"
    );

    tracker.track(task.clone())?;
    Ok(task.task_id)
}

//ingest all .md files in a folder
fn ingest_folder(
    path: &Path,
    _config: &Config,
    tracker: &mut TaskTracker,
) -> anyhow::Result<Vec<String>> {
    let mut ids = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext == "md").unwrap_or(false))
        .collect();

    //sort by filename for deterministic ordering
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let file_path = entry.path();
        match ingest_file(&file_path, tracker) {
            Ok(id) => {
                info!(task_id = %id, file = %file_path.display(), "plan ingested");
                ids.push(id);
            }
            Err(e) => {
                warn!(
                    file = %file_path.display(),
                    error = %e,
                    "failed to ingest plan file, skipping"
                );
            }
        }
    }

    info!(total = ids.len(), "folder ingestion complete");
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    fn sample_plan() -> &'static str {
        r#"# Test Plan

**Task ID:** task_ingest_test_001
**Scheduler:** pulsar-relay

### Component 1: First
**Status:** PENDING
**Agent:** backend-developer

Do something.

### Component 2: Second
**Status:** PENDING
**Agent:** backend-developer

Do another thing.
"#
    }

    fn make_tracker(dir: &Path) -> TaskTracker {
        TaskTracker::new(dir.to_path_buf())
    }

    #[test]
    fn test_ingest_single_file() {
        let tmp_dir = TempDir::new().unwrap();
        let mut tracker = make_tracker(tmp_dir.path());

        let mut f = NamedTempFile::new().unwrap();
        f.write_all(sample_plan().as_bytes()).unwrap();

        let result = ingest_file(f.path(), &mut tracker);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "task_ingest_test_001");
        assert!(tracker.is_tracked("task_ingest_test_001"));
    }

    #[test]
    fn test_ingest_rejects_empty_plan() {
        let tmp_dir = TempDir::new().unwrap();
        let mut tracker = make_tracker(tmp_dir.path());

        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"# Empty Plan\n\n**Task ID:** task_empty\n\nNo components here.\n").unwrap();

        let result = ingest_file(f.path(), &mut tracker);
        assert!(result.is_err());
    }

    #[test]
    fn test_ingest_folder() {
        let tmp_dir = TempDir::new().unwrap();
        let plans_dir = tmp_dir.path().join("plans");
        std::fs::create_dir_all(&plans_dir).unwrap();

        let tracker_dir = tmp_dir.path().join("tracker");
        std::fs::create_dir_all(&tracker_dir).unwrap();
        let mut tracker = make_tracker(&tracker_dir);

        //create two plan files
        std::fs::write(
            plans_dir.join("plan-a.md"),
            r#"# Plan A
**Task ID:** task_folder_a
**Scheduler:** pulsar-relay

### Component 1: Step
**Status:** PENDING
**Agent:** backend-developer

Work.
"#,
        ).unwrap();

        std::fs::write(
            plans_dir.join("plan-b.md"),
            r#"# Plan B
**Task ID:** task_folder_b
**Scheduler:** pulsar-relay

### Phase 1: First
**Status:** PENDING
**Agent:** backend-developer

Work.

### Phase 2: Second
**Status:** PENDING
**Agent:** frontend-developer

More work.
"#,
        ).unwrap();

        //also a non-md file (should be ignored)
        std::fs::write(plans_dir.join("notes.txt"), "ignore me").unwrap();

        let config = crate::config::Config {
            scheduler: crate::config::SchedulerConfig {
                interval_secs: 60,
                pipeline_ready_dir: tmp_dir.path().join("ready"),
                pipeline_processing_dir: tmp_dir.path().join("processing"),
                pipeline_completed_dir: tmp_dir.path().join("completed"),
                task_queue_dir: tracker_dir.clone(),
                lock_file: tmp_dir.path().join("lock"),
            },
            kafka: None,
            agent: crate::config::AgentConfig {
                spawn_script: "/dev/null".into(),
                default_model: "sonnet".into(),
                component_timeout_secs: 30,
            },
        };

        let result = ingest_folder(&plans_dir, &config, &mut tracker);
        assert!(result.is_ok());
        let ids = result.unwrap();
        assert_eq!(ids.len(), 2);
        assert!(tracker.is_tracked("task_folder_a"));
        assert!(tracker.is_tracked("task_folder_b"));
    }

    #[test]
    fn test_ingest_skips_already_tracked() {
        let tmp_dir = TempDir::new().unwrap();
        let mut tracker = make_tracker(tmp_dir.path());

        let mut f = NamedTempFile::new().unwrap();
        f.write_all(sample_plan().as_bytes()).unwrap();

        //ingest once
        ingest_file(f.path(), &mut tracker).unwrap();
        //ingest again — should not error
        let result = ingest_file(f.path(), &mut tracker);
        assert!(result.is_ok());
    }

    #[test]
    fn test_ingest_skips_finished_plan() {
        let tmp_dir = TempDir::new().unwrap();
        let mut tracker = make_tracker(tmp_dir.path());

        let content = r#"# Finished Plan
**Task ID:** task_done
**Scheduler:** pulsar-relay

### Component 1: Done
**Status:** COMPLETED
**Agent:** backend-developer
**Result:** Already done
"#;
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();

        let result = ingest_file(f.path(), &mut tracker);
        assert!(result.is_ok());
        //should not be tracked because it's already finished
        assert!(!tracker.is_tracked("task_done"));
    }
}
