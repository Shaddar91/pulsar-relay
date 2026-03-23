use crate::task_parser::TaskFile;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize)]
struct TrackerState {
    tasks: Vec<TrackedTask>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TrackedTask {
    task_id: String,
    file_path: String,
    total_components: usize,
    completed_components: usize,
    status: String,
    #[serde(default)]
    plan_checksum: Option<String>,
}

pub struct TaskTracker {
    state_dir: PathBuf,
}

impl TaskTracker {
    pub fn new(state_dir: PathBuf) -> Self {
        Self { state_dir }
    }

    fn state_file(&self) -> PathBuf {
        self.state_dir.join("tracker-state.json")
    }

    fn load_state(&self) -> TrackerState {
        let path = self.state_file();
        if path.exists() {
            match std::fs::read_to_string(&path) {
                Ok(content) => {
                    serde_json::from_str(&content).unwrap_or(TrackerState { tasks: vec![] })
                }
                Err(_) => TrackerState { tasks: vec![] },
            }
        } else {
            TrackerState { tasks: vec![] }
        }
    }

    fn save_state(&self, state: &TrackerState) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.state_dir)?;
        let content = serde_json::to_string_pretty(state)?;
        std::fs::write(self.state_file(), content)?;
        Ok(())
    }

    pub fn is_tracked(&self, task_id: &str) -> bool {
        let state = self.load_state();
        state.tasks.iter().any(|t| t.task_id == task_id)
    }

    pub fn track(&mut self, task: TaskFile) -> anyhow::Result<()> {
        let mut state = self.load_state();

        if state.tasks.iter().any(|t| t.task_id == task.task_id) {
            warn!(task_id = %task.task_id, "task already tracked");
            return Ok(());
        }

        let completed = task.components.iter()
            .filter(|c| c.status == crate::task_parser::ComponentStatus::Completed)
            .count();

        state.tasks.push(TrackedTask {
            task_id: task.task_id.clone(),
            file_path: task.path.clone(),
            total_components: task.components.len(),
            completed_components: completed,
            status: "active".to_string(),
            plan_checksum: None,
        });

        self.save_state(&state)?;

        info!(
            task_id = %task.task_id,
            components = task.components.len(),
            "task now tracked"
        );

        Ok(())
    }

    pub fn list_active(&self) -> anyhow::Result<Vec<String>> {
        let state = self.load_state();
        Ok(state.tasks.iter()
            .filter(|t| t.status == "active")
            .map(|t| t.task_id.clone())
            .collect())
    }

    pub fn get_task_path(&self, task_id: &str) -> anyhow::Result<String> {
        let state = self.load_state();
        state.tasks.iter()
            .find(|t| t.task_id == task_id)
            .map(|t| t.file_path.clone())
            .ok_or_else(|| anyhow::anyhow!("task {} not found in tracker", task_id))
    }

    pub fn update_progress(
        &mut self,
        task_id: &str,
        completed: usize,
    ) -> anyhow::Result<()> {
        let mut state = self.load_state();
        if let Some(task) = state.tasks.iter_mut().find(|t| t.task_id == task_id) {
            task.completed_components = completed;
            if completed >= task.total_components {
                task.status = "completed".to_string();
                info!(task_id = %task_id, "task fully completed");
            }
        }
        self.save_state(&state)
    }

    //get the stored checksum for a task's plan file
    pub fn get_checksum(&self, task_id: &str) -> Option<String> {
        let state = self.load_state();
        state.tasks.iter()
            .find(|t| t.task_id == task_id)
            .and_then(|t| t.plan_checksum.clone())
    }

    //store a new checksum for a task's plan file
    pub fn set_checksum(&mut self, task_id: &str, checksum: &str) -> anyhow::Result<()> {
        let mut state = self.load_state();
        if let Some(task) = state.tasks.iter_mut().find(|t| t.task_id == task_id) {
            task.plan_checksum = Some(checksum.to_string());
        }
        self.save_state(&state)
    }

    //mark a task as cancelled (plan removed from active directory)
    pub fn mark_cancelled(&mut self, task_id: &str) -> anyhow::Result<()> {
        let mut state = self.load_state();
        if let Some(task) = state.tasks.iter_mut().find(|t| t.task_id == task_id) {
            task.status = "cancelled".to_string();
            info!(task_id = %task_id, "task marked as cancelled");
        }
        self.save_state(&state)
    }

    //reactivate a cancelled task (plan reappeared in active directory)
    pub fn reactivate(&mut self, task_id: &str) -> anyhow::Result<()> {
        let mut state = self.load_state();
        if let Some(task) = state.tasks.iter_mut().find(|t| t.task_id == task_id) {
            task.status = "active".to_string();
            info!(task_id = %task_id, "task reactivated");
        }
        self.save_state(&state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_task(task_id: &str) -> crate::task_parser::TaskFile {
        crate::task_parser::TaskFile {
            path: "/tmp/test.md".to_string(),
            title: "Test".to_string(),
            task_id: task_id.to_string(),
            components: vec![crate::task_parser::Component {
                index: 1,
                title: "Step".to_string(),
                status: crate::task_parser::ComponentStatus::Pending,
                agent: None,
                content: "Work.".to_string(),
                result: None,
            }],
            scheduler_initiated: true,
        }
    }

    #[test]
    fn test_track_and_list_active() {
        let tmp = tempfile::tempdir().unwrap();
        let mut tracker = TaskTracker::new(tmp.path().to_path_buf());

        let task = make_test_task("task_track_001");
        tracker.track(task).unwrap();

        let active = tracker.list_active().unwrap();
        assert!(active.contains(&"task_track_001".to_string()));
    }

    #[test]
    fn test_mark_cancelled_removes_from_active() {
        let tmp = tempfile::tempdir().unwrap();
        let mut tracker = TaskTracker::new(tmp.path().to_path_buf());

        tracker.track(make_test_task("task_cancel_001")).unwrap();
        assert!(tracker.list_active().unwrap().contains(&"task_cancel_001".to_string()));

        tracker.mark_cancelled("task_cancel_001").unwrap();
        assert!(!tracker.list_active().unwrap().contains(&"task_cancel_001".to_string()));
    }

    #[test]
    fn test_reactivate_after_cancel() {
        let tmp = tempfile::tempdir().unwrap();
        let mut tracker = TaskTracker::new(tmp.path().to_path_buf());

        tracker.track(make_test_task("task_react_001")).unwrap();
        tracker.mark_cancelled("task_react_001").unwrap();
        assert!(!tracker.list_active().unwrap().contains(&"task_react_001".to_string()));

        tracker.reactivate("task_react_001").unwrap();
        assert!(tracker.list_active().unwrap().contains(&"task_react_001".to_string()));
    }

    #[test]
    fn test_is_tracked_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let mut tracker = TaskTracker::new(tmp.path().to_path_buf());

        assert!(!tracker.is_tracked("task_persist_001"));
        tracker.track(make_test_task("task_persist_001")).unwrap();
        assert!(tracker.is_tracked("task_persist_001"));
    }

    #[test]
    fn test_checksum_operations() {
        let tmp = tempfile::tempdir().unwrap();
        let mut tracker = TaskTracker::new(tmp.path().to_path_buf());

        tracker.track(make_test_task("task_cksum_001")).unwrap();
        assert!(tracker.get_checksum("task_cksum_001").is_none());

        tracker.set_checksum("task_cksum_001", "abc123").unwrap();
        assert_eq!(tracker.get_checksum("task_cksum_001").unwrap(), "abc123");
    }
}
