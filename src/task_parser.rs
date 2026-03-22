use serde::{Deserialize, Serialize};
use std::path::Path;
use regex::Regex;
use tracing::debug;

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
            .unwrap_or_else(|| format!("task_{}", uuid::Uuid::new_v4()));

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
        let re = Regex::new(r"(?m)^### Component (\d+):\s*(.+)$")?;
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

            //extract result if present
            let result = if section.contains("**Result:**") {
                let result_start = section.find("**Result:**").unwrap() + "**Result:**".len();
                Some(section[result_start..].trim().to_string())
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
        let header_pattern = format!("### Component {}:", component_index);

        let mut lines: Vec<String> = content.lines().map(String::from).collect();
        let mut in_component = false;
        let mut status_updated = false;

        for i in 0..lines.len() {
            if lines[i].contains(&header_pattern) {
                in_component = true;
                continue;
            }
            if in_component && lines[i].starts_with("### Component ") {
                //reached next component, insert result before it if needed
                if let Some(res) = result {
                    if !status_updated {
                        lines.insert(i, format!("**Result:** {}\n", res));
                    }
                }
                break;
            }
            if in_component && lines[i].contains("**Status:**") {
                lines[i] = format!("**Status:** {}", new_status);
                status_updated = true;
            }
        }

        //if we're at the last component and have a result to add
        if in_component && !status_updated {
            //status line not found, append to end
        }
        if let Some(res) = result {
            if in_component {
                lines.push(format!("\n**Result:** {}", res));
            }
        }

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
}
