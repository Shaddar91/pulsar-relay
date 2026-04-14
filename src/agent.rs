use crate::config::AgentConfig;
use crate::task_parser::PlanKind;
use std::path::Path;
use std::process::Command;
use tracing::info;

//pure — map a plan kind to its prompt template filename
pub fn template_filename(kind: PlanKind) -> &'static str {
    match kind {
        PlanKind::Execution => "prep-execution.md",
        PlanKind::Research => "prep-research.md",
    }
}

//pure — substitute {{key}} placeholders in a template with values
//unknown placeholders are left as-is (deliberate: makes debugging easier)
pub fn render_template(template: &str, vars: &[(&str, &str)]) -> String {
    vars.iter().fold(template.to_string(), |acc, (key, value)| {
        acc.replace(&format!("{{{{{}}}}}", key), value)
    })
}

//IO — load a prompt template from disk for a given plan kind
pub fn load_template(dir: &Path, kind: PlanKind) -> anyhow::Result<String> {
    let path = dir.join(template_filename(kind));
    std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("failed to load template {}: {}", path.display(), e))
}

//IO — invoke spawn-agent-v2.sh with a rendered prompt
//pipeline_ready_dir is exported as PULSAR_PIPELINE_READY_DIR so the prep agent
//knows where to drop pipeline task files (works for both real Claude agents via
//their Bash tool and for the test mock via env expansion)
//returns the agent's stdout; caller parses the STATUS= line
async fn invoke_spawn_script(
    script: &str,
    agent_name: &str,
    model: &str,
    prompt: &str,
    pipeline_ready_dir: &str,
    timeout_secs: u64,
) -> anyhow::Result<String> {
    let script = script.to_string();
    let agent = agent_name.to_string();
    let model = model.to_string();
    let prompt = prompt.to_string();
    let ready_dir = pipeline_ready_dir.to_string();

    let spawn_handle = tokio::task::spawn_blocking(move || {
        Command::new("bash")
            .arg(&script)
            .arg("--agent")
            .arg(&agent)
            .arg("--model")
            .arg(&model)
            .arg("--prompt")
            .arg(&prompt)
            .env("PULSAR_RELAY_TASK", "true")
            .env("PULSAR_PIPELINE_READY_DIR", &ready_dir)
            .output()
    });

    let output = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        spawn_handle,
    )
    .await
    .map_err(|_| anyhow::anyhow!("prep agent timed out after {}s", timeout_secs))???;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        Err(anyhow::anyhow!(
            "prep agent exited with {}: {}",
            output.status,
            stderr
        ))
    }
}

//pure — parse the first-line STATUS= contract from agent stdout
//returns None if the output doesn't start with STATUS=
pub fn parse_status_line(stdout: &str) -> Option<&str> {
    stdout.lines().next().and_then(|l| l.strip_prefix("STATUS="))
}

//coordinator — composes load + render + spawn for one prep-agent invocation
pub struct PrepAgentSpawner {
    config: AgentConfig,
}

impl PrepAgentSpawner {
    pub fn new(config: AgentConfig) -> Self {
        Self { config }
    }

    //spawn the prep agent for a given plan + kind + target pipeline dir
    //the agent reads the plan, retrieves prior pipeline completions, dispatches next component, exits
    pub async fn spawn(
        &self,
        plan_path: &Path,
        plan_kind: PlanKind,
        pipeline_ready_dir: &Path,
    ) -> anyhow::Result<String> {
        let template = load_template(&self.config.prep_template_dir, plan_kind)?;
        let plan_path_str = plan_path.to_string_lossy().to_string();
        let plan_kind_str = plan_kind.to_string();
        let ready_dir_str = pipeline_ready_dir.to_string_lossy().to_string();
        let vars = [
            ("plan_path", plan_path_str.as_str()),
            ("plan_kind", plan_kind_str.as_str()),
            ("pipeline_ready_dir", ready_dir_str.as_str()),
        ];
        let rendered = render_template(&template, &vars);

        info!(
            plan_path = %plan_path.display(),
            plan_kind = %plan_kind,
            agent = %self.config.prep_agent_name,
            "spawning prep agent"
        );

        let stdout = invoke_spawn_script(
            &self.config.spawn_script.to_string_lossy(),
            &self.config.prep_agent_name,
            &self.config.default_model,
            &rendered,
            &ready_dir_str,
            self.config.prep_timeout_secs,
        )
        .await?;

        info!(
            plan_path = %plan_path.display(),
            output_len = stdout.len(),
            "prep agent completed"
        );

        Ok(stdout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_template_filename_execution() {
        assert_eq!(template_filename(PlanKind::Execution), "prep-execution.md");
    }

    #[test]
    fn test_template_filename_research() {
        assert_eq!(template_filename(PlanKind::Research), "prep-research.md");
    }

    #[test]
    fn test_render_template_single_var() {
        let tpl = "Hello {{name}}";
        let out = render_template(tpl, &[("name", "world")]);
        assert_eq!(out, "Hello world");
    }

    #[test]
    fn test_render_template_multiple_vars() {
        let tpl = "Plan at {{path}} is kind {{kind}}";
        let out = render_template(tpl, &[("path", "/tmp/plan.md"), ("kind", "EXECUTION")]);
        assert_eq!(out, "Plan at /tmp/plan.md is kind EXECUTION");
    }

    #[test]
    fn test_render_template_repeated_var() {
        let tpl = "{{x}} and {{x}} again";
        let out = render_template(tpl, &[("x", "foo")]);
        assert_eq!(out, "foo and foo again");
    }

    #[test]
    fn test_render_template_unknown_var_kept_as_is() {
        let tpl = "Hello {{name}} — {{mystery}}";
        let out = render_template(tpl, &[("name", "world")]);
        assert_eq!(out, "Hello world — {{mystery}}");
    }

    #[test]
    fn test_render_template_no_vars() {
        let tpl = "plain text";
        let out = render_template(tpl, &[]);
        assert_eq!(out, "plain text");
    }

    #[test]
    fn test_load_template_missing_file_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let result = load_template(tmp.path(), PlanKind::Execution);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_template_existing_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("prep-execution.md");
        std::fs::write(&path, "hello {{x}}").unwrap();
        let result = load_template(tmp.path(), PlanKind::Execution).unwrap();
        assert_eq!(result, "hello {{x}}");
    }

    #[test]
    fn test_load_template_picks_right_file_for_kind() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("prep-execution.md"), "exec").unwrap();
        std::fs::write(tmp.path().join("prep-research.md"), "research").unwrap();
        assert_eq!(load_template(tmp.path(), PlanKind::Execution).unwrap(), "exec");
        assert_eq!(load_template(tmp.path(), PlanKind::Research).unwrap(), "research");
    }

    #[test]
    fn test_parse_status_line_ok() {
        assert_eq!(parse_status_line("STATUS=ok dispatched task_foo\nextra line"), Some("ok dispatched task_foo"));
    }

    #[test]
    fn test_parse_status_line_failed() {
        assert_eq!(parse_status_line("STATUS=failed missing plan"), Some("failed missing plan"));
    }

    #[test]
    fn test_parse_status_line_no_prefix() {
        assert_eq!(parse_status_line("not a status line"), None);
    }

    #[test]
    fn test_parse_status_line_empty() {
        assert_eq!(parse_status_line(""), None);
    }
}
