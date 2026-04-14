use serde::{Deserialize, Serialize};
use std::path::Path;
use regex::Regex;
use tracing::debug;

//check if a line is a component/phase header
fn is_section_header(line: &str) -> bool {
    line.starts_with("### Component ") || line.starts_with("### Phase ")
}

//find the line range for a component section: (header_line, next_header_line_or_none)
fn find_component_bounds(content: &str, component_index: usize) -> (Option<usize>, Option<usize>) {
    let lines: Vec<&str> = content.lines().collect();
    let component_pattern = format!("### Component {}:", component_index);
    let phase_pattern = format!("### Phase {}:", component_index);
    let mut start = None;
    for (i, line) in lines.iter().enumerate() {
        if start.is_none() && (line.contains(&component_pattern) || line.contains(&phase_pattern)) {
            start = Some(i);
            continue;
        }
        if start.is_some() && is_section_header(line) {
            return (start, Some(i));
        }
    }
    (start, None)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFile {
    pub path: String,
    pub title: String,
    pub task_id: String,
    pub components: Vec<Component>,
    pub scheduler_initiated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Component {
    pub index: usize,
    pub title: String,
    pub status: ComponentStatus,
    pub agent: Option<String>,
    pub content: String,
    pub result: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ComponentStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum PlanKind {
    Execution,
    Research,
}

impl std::fmt::Display for PlanKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanKind::Execution => write!(f, "EXECUTION"),
            PlanKind::Research => write!(f, "RESEARCH"),
        }
    }
}

//extract the **Plan Kind:** value from a plan's markdown content
pub fn extract_plan_kind(content: &str) -> Option<PlanKind> {
    content.lines()
        .find(|l| l.starts_with("**Plan Kind:**"))
        .and_then(|l| {
            let v = l.split("**Plan Kind:**").nth(1)?.trim();
            match v {
                "EXECUTION" => Some(PlanKind::Execution),
                "RESEARCH" => Some(PlanKind::Research),
                _ => None,
            }
        })
}

//extract the **Project:** field verbatim (does NOT parse [[...]] wikilinks —
//pulsar passes the value through to the pipeline as opaque metadata)
pub fn extract_project(content: &str) -> Option<String> {
    content.lines()
        .find(|l| l.starts_with("**Project:**"))
        .map(|l| l.split("**Project:**").nth(1).unwrap_or("").trim().to_string())
        .filter(|s| !s.is_empty())
}

impl std::fmt::Display for ComponentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentStatus::Pending => write!(f, "PENDING"),
            ComponentStatus::InProgress => write!(f, "IN_PROGRESS"),
            ComponentStatus::Completed => write!(f, "COMPLETED"),
            ComponentStatus::Failed => write!(f, "FAILED"),
        }
    }
}

impl TaskFile {
    //parse a multi-component task file
    //expects components marked as:
    //  ### Component N: Title
    //  **Status:** PENDING|IN_PROGRESS|COMPLETED|FAILED
    //  **Agent:** agent-name (optional)
    //  content...
    pub fn parse(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let lines: Vec<&str> = content.lines().collect();

        let title = lines.iter()
            .find(|l| l.starts_with("# "))
            .map(|l| l.trim_start_matches("# ").to_string())
            .unwrap_or_else(|| "Untitled".to_string());

        let task_id = Self::extract_field(&content, "Task ID")
            .unwrap_or_else(|| {
                //deterministic ID from filename — stable across repeated parses
                let stem = path.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown");
                format!("task_{}", stem)
            });

        let scheduler_initiated = content.contains("**Scheduler:** pulsar-relay");

        let components = Self::parse_components(&content)?;

        debug!(
            task_id = %task_id,
            components = components.len(),
            "parsed task file"
        );

        Ok(TaskFile {
            path: path.to_string_lossy().to_string(),
            title,
            task_id,
            components,
            scheduler_initiated,
        })
    }

    fn parse_components(content: &str) -> anyhow::Result<Vec<Component>> {
        //support both ### Component N: and ### Phase N: headers
        let re = Regex::new(r"(?m)^### (?:Component|Phase) (\d+):\s*(.+)$")?;
        let status_re = Regex::new(r"\*\*Status:\*\*\s*(\w+)")?;
        let agent_re = Regex::new(r"\*\*Agent:\*\*\s*(\S+)")?;

        let mut components = Vec::new();
        let matches: Vec<_> = re.find_iter(content).collect();

        for (i, m) in matches.iter().enumerate() {
            let caps = re.captures(m.as_str()).unwrap();
            let index: usize = caps[1].parse().unwrap_or(i + 1);
            let title = caps[2].trim().to_string();

            //extract section content (from this header to next component or end)
            let start = m.end();
            let end = matches.get(i + 1)
                .map(|next| next.start())
                .unwrap_or(content.len());
            let section = &content[start..end];

            let status = status_re.captures(section)
                .map(|c| match c[1].to_uppercase().as_str() {
                    "PENDING" => ComponentStatus::Pending,
                    "IN_PROGRESS" => ComponentStatus::InProgress,
                    "COMPLETED" => ComponentStatus::Completed,
                    "FAILED" => ComponentStatus::Failed,
                    _ => ComponentStatus::Pending,
                })
                .unwrap_or(ComponentStatus::Pending);

            let agent = agent_re.captures(section)
                .map(|c| c[1].to_string());

            //extract result if present (single line only)
            let result = if let Some(result_line) = section.lines().find(|l| l.contains("**Result:**")) {
                let val = result_line.split("**Result:**").nth(1).unwrap_or("").trim();
                Some(val.to_string())
            } else {
                None
            };

            components.push(Component {
                index,
                title,
                status,
                agent,
                content: section.trim().to_string(),
                result,
            });
        }

        Ok(components)
    }

    fn extract_field(content: &str, field: &str) -> Option<String> {
        let pattern = format!("**{}:**", field);
        content.lines()
            .find(|l| l.contains(&pattern))
            .map(|l| {
                l.split(&pattern)
                    .nth(1)
                    .unwrap_or("")
                    .trim()
                    .to_string()
            })
    }

    //find the next pending component
    pub fn next_pending(&self) -> Option<&Component> {
        self.components.iter()
            .find(|c| c.status == ComponentStatus::Pending)
    }

    //check if any component is currently in progress
    pub fn has_in_progress(&self) -> bool {
        self.components.iter()
            .any(|c| c.status == ComponentStatus::InProgress)
    }

    //check if all components are done (completed or failed)
    pub fn is_finished(&self) -> bool {
        self.components.iter()
            .all(|c| c.status == ComponentStatus::Completed || c.status == ComponentStatus::Failed)
    }

    //update a component's status in the original file
    pub fn update_component_status(
        path: &Path,
        component_index: usize,
        new_status: ComponentStatus,
        result: Option<&str>,
    ) -> anyhow::Result<()> {
        let content = std::fs::read_to_string(path)?;

        let (section_start, section_end) = find_component_bounds(&content, component_index);
        if section_start.is_none() {
            return Err(anyhow::anyhow!(
                "component {} not found in {}",
                component_index,
                path.display()
            ));
        }

        let section_start = section_start.unwrap();
        let mut lines: Vec<String> = content.lines().map(String::from).collect();

        //update status line and find last metadata line in component
        let bound = section_end.unwrap_or(lines.len());
        let mut last_meta_idx = section_start;
        for i in section_start..bound {
            if lines[i].contains("**Status:**") {
                lines[i] = format!("**Status:** {}", new_status);
                last_meta_idx = i;
            }
            if lines[i].contains("**Agent:**") {
                last_meta_idx = i;
            }
        }

        //remove existing result lines within component bounds
        if result.is_some() {
            let mut i = section_start;
            let cur_bound = section_end.unwrap_or(lines.len());
            while i < cur_bound && i < lines.len() {
                if lines[i].contains("**Result:**") {
                    lines.remove(i);
                    //adjust last_meta_idx if we removed a line before/at it
                    if i <= last_meta_idx {
                        last_meta_idx = last_meta_idx.saturating_sub(1);
                    }
                } else {
                    i += 1;
                }
            }
        }

        //insert result after the last metadata line (Status/Agent)
        if let Some(res) = result {
            lines.insert(last_meta_idx + 1, format!("**Result:** {}", res));
        }

        std::fs::write(path, lines.join("\n"))?;
        Ok(())
    }

    //update the plan header with overall progress metadata
    pub fn update_plan_header(path: &Path) -> anyhow::Result<()> {
        let task = Self::parse(path)?;
        let total = task.components.len();
        let completed = task.components.iter()
            .filter(|c| c.status == ComponentStatus::Completed)
            .count();
        let failed = task.components.iter()
            .filter(|c| c.status == ComponentStatus::Failed)
            .count();
        let in_progress = task.components.iter()
            .filter(|c| c.status == ComponentStatus::InProgress)
            .count();

        let overall_status = if task.is_finished() {
            "COMPLETED".to_string()
        } else if in_progress > 0 {
            format!("IN_PROGRESS ({}/{} done)", completed + failed, total)
        } else if completed > 0 || failed > 0 {
            format!("IN_PROGRESS ({}/{} done)", completed + failed, total)
        } else {
            "PENDING".to_string()
        };

        let progress_line = format!(
            "**Progress:** {}/{} components ({} completed, {} failed)",
            completed + failed, total, completed, failed
        );
        let status_line = format!("**Plan Status:** {}", overall_status);

        let content = std::fs::read_to_string(path)?;
        let mut lines: Vec<String> = content.lines().map(String::from).collect();

        //remove existing plan metadata lines
        lines.retain(|l| !l.starts_with("**Plan Status:**") && !l.starts_with("**Progress:**"));

        //insert after the title line (first # line)
        let insert_idx = lines.iter()
            .position(|l| l.starts_with("# "))
            .map(|i| i + 1)
            .unwrap_or(0);

        lines.insert(insert_idx, progress_line);
        lines.insert(insert_idx, status_line);

        std::fs::write(path, lines.join("\n"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_components() {
        let content = r#"# Large Task

**Task ID:** task_test_001
**Scheduler:** pulsar-relay

## Components

### Component 1: Set up database schema
**Status:** COMPLETED
**Agent:** backend-developer
**Result:** Schema created with 5 tables

### Component 2: Build API endpoints
**Status:** PENDING
**Agent:** backend-developer

Implement REST endpoints for CRUD operations.

### Component 3: Create frontend views
**Status:** PENDING
**Agent:** frontend-developer

Build React components for the dashboard.
"#;

        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();

        let task = TaskFile::parse(f.path()).unwrap();
        assert_eq!(task.components.len(), 3);
        assert_eq!(task.components[0].status, ComponentStatus::Completed);
        assert_eq!(task.components[1].status, ComponentStatus::Pending);
        assert!(task.scheduler_initiated);
        assert_eq!(task.next_pending().unwrap().index, 2);
    }

    fn make_task_file(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    fn sample_3_component_task() -> &'static str {
        r#"# Test Task

**Task ID:** task_test_002
**Scheduler:** pulsar-relay

## Components

### Component 1: First step
**Status:** PENDING
**Agent:** backend-developer

Do the first thing.

### Component 2: Second step
**Status:** PENDING
**Agent:** backend-developer

Do the second thing.

### Component 3: Third step
**Status:** PENDING
**Agent:** frontend-developer

Do the third thing.
"#
    }

    #[test]
    fn test_update_status_marks_in_progress() {
        let f = make_task_file(sample_3_component_task());
        TaskFile::update_component_status(
            f.path(), 1, ComponentStatus::InProgress, None,
        ).unwrap();

        let task = TaskFile::parse(f.path()).unwrap();
        assert_eq!(task.components[0].status, ComponentStatus::InProgress);
        assert_eq!(task.components[1].status, ComponentStatus::Pending);
    }

    #[test]
    fn test_update_status_with_result_mid_component() {
        let f = make_task_file(sample_3_component_task());
        TaskFile::update_component_status(
            f.path(), 1, ComponentStatus::Completed, Some("Done: created 3 files"),
        ).unwrap();

        let task = TaskFile::parse(f.path()).unwrap();
        assert_eq!(task.components[0].status, ComponentStatus::Completed);
        assert_eq!(task.components[0].result.as_deref(), Some("Done: created 3 files"));
        //component 2 should still be pending and intact
        assert_eq!(task.components[1].status, ComponentStatus::Pending);
        assert_eq!(task.components[1].title, "Second step");
    }

    #[test]
    fn test_update_status_with_result_last_component() {
        let f = make_task_file(sample_3_component_task());
        TaskFile::update_component_status(
            f.path(), 3, ComponentStatus::Completed, Some("All done"),
        ).unwrap();

        let task = TaskFile::parse(f.path()).unwrap();
        assert_eq!(task.components[2].status, ComponentStatus::Completed);
        assert_eq!(task.components[2].result.as_deref(), Some("All done"));
    }

    #[test]
    fn test_sequential_component_updates() {
        let f = make_task_file(sample_3_component_task());

        //mark component 1 in-progress then completed
        TaskFile::update_component_status(
            f.path(), 1, ComponentStatus::InProgress, None,
        ).unwrap();
        TaskFile::update_component_status(
            f.path(), 1, ComponentStatus::Completed, Some("Step 1 result"),
        ).unwrap();

        //mark component 2 in-progress then failed
        TaskFile::update_component_status(
            f.path(), 2, ComponentStatus::InProgress, None,
        ).unwrap();
        TaskFile::update_component_status(
            f.path(), 2, ComponentStatus::Failed, Some("Error: timeout"),
        ).unwrap();

        let task = TaskFile::parse(f.path()).unwrap();
        assert_eq!(task.components[0].status, ComponentStatus::Completed);
        assert_eq!(task.components[0].result.as_deref(), Some("Step 1 result"));
        assert_eq!(task.components[1].status, ComponentStatus::Failed);
        assert_eq!(task.components[1].result.as_deref(), Some("Error: timeout"));
        assert_eq!(task.components[2].status, ComponentStatus::Pending);
        assert!(!task.is_finished());
    }

    #[test]
    fn test_is_finished_when_all_done() {
        let f = make_task_file(sample_3_component_task());

        for i in 1..=3 {
            TaskFile::update_component_status(
                f.path(), i, ComponentStatus::Completed, Some(&format!("Result {}", i)),
            ).unwrap();
        }

        let task = TaskFile::parse(f.path()).unwrap();
        assert!(task.is_finished());
        assert!(task.next_pending().is_none());
    }

    #[test]
    fn test_has_in_progress() {
        let f = make_task_file(sample_3_component_task());
        TaskFile::update_component_status(
            f.path(), 1, ComponentStatus::InProgress, None,
        ).unwrap();

        let task = TaskFile::parse(f.path()).unwrap();
        assert!(task.has_in_progress());
    }

    #[test]
    fn test_parse_phase_headers() {
        let content = r#"# Phase-Based Plan

**Task ID:** task_phase_001
**Scheduler:** pulsar-relay

## Components

### Phase 1: Database Setup
**Status:** COMPLETED
**Agent:** backend-developer
**Result:** Tables created

### Phase 2: API Layer
**Status:** PENDING
**Agent:** backend-developer

Build the REST API.

### Phase 3: Frontend
**Status:** PENDING
**Agent:** frontend-developer

Build the UI.
"#;
        let f = make_task_file(content);
        let task = TaskFile::parse(f.path()).unwrap();
        assert_eq!(task.components.len(), 3);
        assert_eq!(task.components[0].status, ComponentStatus::Completed);
        assert_eq!(task.components[0].title, "Database Setup");
        assert_eq!(task.components[1].status, ComponentStatus::Pending);
        assert_eq!(task.next_pending().unwrap().index, 2);
    }

    #[test]
    fn test_update_phase_header_status() {
        let content = r#"# Phase Plan

**Task ID:** task_phase_update
**Scheduler:** pulsar-relay

### Phase 1: Step one
**Status:** PENDING
**Agent:** backend-developer

Do it.

### Phase 2: Step two
**Status:** PENDING
**Agent:** backend-developer

Do it too.
"#;
        let f = make_task_file(content);
        TaskFile::update_component_status(
            f.path(), 1, ComponentStatus::Completed, Some("Done"),
        ).unwrap();

        let task = TaskFile::parse(f.path()).unwrap();
        assert_eq!(task.components[0].status, ComponentStatus::Completed);
        assert_eq!(task.components[0].result.as_deref(), Some("Done"));
        assert_eq!(task.components[1].status, ComponentStatus::Pending);
    }

    #[test]
    fn test_update_plan_header() {
        let f = make_task_file(sample_3_component_task());
        TaskFile::update_component_status(
            f.path(), 1, ComponentStatus::Completed, Some("OK"),
        ).unwrap();
        TaskFile::update_plan_header(f.path()).unwrap();

        let content = std::fs::read_to_string(f.path()).unwrap();
        assert!(content.contains("**Plan Status:**"), "Should have Plan Status");
        assert!(content.contains("**Progress:** 1/3"), "Should show 1/3 progress");
    }

    #[test]
    fn test_plan_header_completed() {
        let f = make_task_file(sample_3_component_task());
        for i in 1..=3 {
            TaskFile::update_component_status(
                f.path(), i, ComponentStatus::Completed, Some(&format!("R{}", i)),
            ).unwrap();
        }
        TaskFile::update_plan_header(f.path()).unwrap();

        let content = std::fs::read_to_string(f.path()).unwrap();
        assert!(content.contains("**Plan Status:** COMPLETED"), "Should be COMPLETED");
        assert!(content.contains("**Progress:** 3/3"), "Should show 3/3");
    }

    #[test]
    fn test_failed_component_does_not_block_finished() {
        let content = r#"# Test Task

**Task ID:** task_fail_test
**Scheduler:** pulsar-relay

## Components

### Component 1: Works
**Status:** COMPLETED
**Agent:** backend-developer
**Result:** OK

### Component 2: Fails
**Status:** FAILED
**Agent:** backend-developer
**Result:** Error: something broke
"#;
        let f = make_task_file(content);
        let task = TaskFile::parse(f.path()).unwrap();
        assert!(task.is_finished());
        assert!(task.next_pending().is_none());
    }

    //--- PlanKind / extract_plan_kind / extract_project tests ---

    #[test]
    fn test_extract_plan_kind_execution() {
        let content = "# Some Plan\n**Scheduler:** pulsar-relay\n**Plan Kind:** EXECUTION\n";
        assert_eq!(extract_plan_kind(content), Some(PlanKind::Execution));
    }

    #[test]
    fn test_extract_plan_kind_research() {
        let content = "# Some Plan\n**Plan Kind:** RESEARCH\n";
        assert_eq!(extract_plan_kind(content), Some(PlanKind::Research));
    }

    #[test]
    fn test_extract_plan_kind_missing() {
        let content = "# No Kind\n**Scheduler:** pulsar-relay\n";
        assert_eq!(extract_plan_kind(content), None);
    }

    #[test]
    fn test_extract_plan_kind_unknown_value() {
        let content = "**Plan Kind:** BOGUS\n";
        assert_eq!(extract_plan_kind(content), None);
    }

    #[test]
    fn test_extract_plan_kind_display() {
        assert_eq!(format!("{}", PlanKind::Execution), "EXECUTION");
        assert_eq!(format!("{}", PlanKind::Research), "RESEARCH");
    }

    #[test]
    fn test_extract_project_wikilink() {
        let content = "# Plan\n**Project:** [[authentic-web]]\n";
        assert_eq!(extract_project(content), Some("[[authentic-web]]".to_string()));
    }

    #[test]
    fn test_extract_project_plain() {
        let content = "**Project:** pulsar-relay\n";
        assert_eq!(extract_project(content), Some("pulsar-relay".to_string()));
    }

    #[test]
    fn test_extract_project_missing() {
        let content = "# No project\n";
        assert_eq!(extract_project(content), None);
    }

    #[test]
    fn test_extract_project_empty_is_none() {
        let content = "**Project:**   \n";
        assert_eq!(extract_project(content), None);
    }
}
