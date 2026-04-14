use crate::config::Config;
use crate::task_parser::TaskFile;
use crate::tracker::TaskTracker;
use std::path::Path;
use tracing::{info, warn};

//ingest a plan file or folder of plans into the scheduler
pub async fn ingest_path(
    path: &Path,
    config: &Config,
    tracker: &mut TaskTracker,
) -> anyhow::Result<Vec<String>> {
    if path.is_file() {
        let id = ingest_file(path, config, tracker).await?;
        Ok(vec![id])
    } else if path.is_dir() {
        ingest_folder(path, config, tracker).await
    } else {
        Err(anyhow::anyhow!("path does not exist: {}", path.display()))
    }
}

//ingest a single plan file — validate, register with tracker, publish to kafka
//note: checksum is NOT stored here — the scheduler sets it after processing
pub async fn ingest_file(
    path: &Path,
    config: &Config,
    tracker: &mut TaskTracker,
) -> anyhow::Result<String> {
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

    //publish pending components to kafka if available (non-fatal)
    publish_to_kafka_if_available(config, &task, &abs_path).await;

    Ok(task.task_id)
}

//publish pending components to kafka (non-fatal on failure)
async fn publish_to_kafka_if_available(
    config: &Config,
    task: &TaskFile,
    plan_path: &Path,
) {
    #[cfg(feature = "kafka")]
    {
        use crate::task_parser::ComponentStatus;
        use crate::checksum;

        if let Some(ref kafka_config) = config.kafka {
            match crate::kafka::producer::TaskProducer::new(kafka_config) {
                Ok(producer) => {
                    let hash = checksum::hash_file(plan_path).unwrap_or_default();
                    let pending: Vec<_> = task.components.iter()
                        .filter(|c| c.status == ComponentStatus::Pending)
                        .collect();

                    for component in &pending {
                        if let Err(e) = producer.send_dispatch(
                            &task.task_id,
                            component,
                            &plan_path.to_string_lossy(),
                            &hash,
                        ).await {
                            warn!(
                                task_id = %task.task_id,
                                component = component.index,
                                error = %e,
                                "failed to publish component to kafka (non-fatal)"
                            );
                        }
                    }
                    info!(
                        task_id = %task.task_id,
                        count = pending.len(),
                        "published pending components to kafka"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "kafka unavailable — operating in local-only mode");
                }
            }
        }
    }

    #[cfg(not(feature = "kafka"))]
    {
        let _ = (config, task, plan_path);
    }
}

//ingest all .md files in a folder
async fn ingest_folder(
    path: &Path,
    config: &Config,
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
        match ingest_file(&file_path, config, tracker).await {
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

    fn test_config(tmp_dir: &Path) -> Config {
        Config {
            scheduler: crate::config::SchedulerConfig {
                interval_secs: 60,
                pipeline_ready_dir: tmp_dir.join("ready"),
                pipeline_processing_dir: tmp_dir.join("processing"),
                pipeline_completed_dir: tmp_dir.join("completed"),
                task_queue_dir: tmp_dir.join("tasks"),
                lock_file: tmp_dir.join("lock"),
            },
            kafka: None,
            plans: None,
            telegram: None,
            agent: crate::config::AgentConfig {
                spawn_script: "/dev/null".into(),
                default_model: "sonnet".into(),
                prep_timeout_secs: 30,
                prep_template_dir: "/tmp/pulsar-prompts".into(),
                prep_agent_name: "pulsar-prep".into(),
            },
        }
    }

    #[tokio::test]
    async fn test_ingest_single_file() {
        let tmp_dir = TempDir::new().unwrap();
        let mut tracker = make_tracker(tmp_dir.path());
        let config = test_config(tmp_dir.path());

        let mut f = NamedTempFile::new().unwrap();
        f.write_all(sample_plan().as_bytes()).unwrap();

        let result = ingest_file(f.path(), &config, &mut tracker).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "task_ingest_test_001");
        assert!(tracker.is_tracked("task_ingest_test_001"));
    }

    #[tokio::test]
    async fn test_ingest_no_checksum_stored() {
        //checksum is NOT set during ingestion — scheduler owns that
        let tmp_dir = TempDir::new().unwrap();
        let mut tracker = make_tracker(tmp_dir.path());
        let config = test_config(tmp_dir.path());

        let mut f = NamedTempFile::new().unwrap();
        f.write_all(sample_plan().as_bytes()).unwrap();

        ingest_file(f.path(), &config, &mut tracker).await.unwrap();

        let checksum = tracker.get_checksum("task_ingest_test_001");
        assert!(checksum.is_none());
    }

    #[tokio::test]
    async fn test_ingest_rejects_empty_plan() {
        let tmp_dir = TempDir::new().unwrap();
        let mut tracker = make_tracker(tmp_dir.path());
        let config = test_config(tmp_dir.path());

        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"# Empty Plan\n\n**Task ID:** task_empty\n\nNo components here.\n").unwrap();

        let result = ingest_file(f.path(), &config, &mut tracker).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ingest_folder() {
        let tmp_dir = TempDir::new().unwrap();
        let plans_dir = tmp_dir.path().join("plans");
        std::fs::create_dir_all(&plans_dir).unwrap();

        let tracker_dir = tmp_dir.path().join("tracker");
        std::fs::create_dir_all(&tracker_dir).unwrap();
        let mut tracker = make_tracker(&tracker_dir);

        let config = Config {
            scheduler: crate::config::SchedulerConfig {
                interval_secs: 60,
                pipeline_ready_dir: tmp_dir.path().join("ready"),
                pipeline_processing_dir: tmp_dir.path().join("processing"),
                pipeline_completed_dir: tmp_dir.path().join("completed"),
                task_queue_dir: tracker_dir.clone(),
                lock_file: tmp_dir.path().join("lock"),
            },
            kafka: None,
            plans: None,
            telegram: None,
            agent: crate::config::AgentConfig {
                spawn_script: "/dev/null".into(),
                default_model: "sonnet".into(),
                prep_timeout_secs: 30,
                prep_template_dir: "/tmp/pulsar-prompts".into(),
                prep_agent_name: "pulsar-prep".into(),
            },
        };

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

        //non-md file (should be ignored)
        std::fs::write(plans_dir.join("notes.txt"), "ignore me").unwrap();

        let result = ingest_folder(&plans_dir, &config, &mut tracker).await;
        assert!(result.is_ok());
        let ids = result.unwrap();
        assert_eq!(ids.len(), 2);
        assert!(tracker.is_tracked("task_folder_a"));
        assert!(tracker.is_tracked("task_folder_b"));
    }

    #[tokio::test]
    async fn test_ingest_skips_already_tracked() {
        let tmp_dir = TempDir::new().unwrap();
        let mut tracker = make_tracker(tmp_dir.path());
        let config = test_config(tmp_dir.path());

        let mut f = NamedTempFile::new().unwrap();
        f.write_all(sample_plan().as_bytes()).unwrap();

        ingest_file(f.path(), &config, &mut tracker).await.unwrap();
        let result = ingest_file(f.path(), &config, &mut tracker).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_ingest_skips_finished_plan() {
        let tmp_dir = TempDir::new().unwrap();
        let mut tracker = make_tracker(tmp_dir.path());
        let config = test_config(tmp_dir.path());

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

        let result = ingest_file(f.path(), &config, &mut tracker).await;
        assert!(result.is_ok());
        assert!(!tracker.is_tracked("task_done"));
    }
}
