use regex::Regex;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct Config {
    pub scheduler: SchedulerConfig,
    #[serde(default)]
    pub kafka: Option<KafkaConfig>,
    #[serde(default)]
    pub plans: Option<PlansConfig>,
    pub agent: AgentConfig,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct SchedulerConfig {
    pub interval_secs: u64,
    pub pipeline_ready_dir: PathBuf,
    pub pipeline_processing_dir: PathBuf,
    pub pipeline_completed_dir: PathBuf,
    pub task_queue_dir: PathBuf,
    pub lock_file: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct KafkaConfig {
    pub brokers: String,
    pub task_topic: String,
    pub result_topic: String,
    pub group_id: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PlansConfig {
    pub drafts_dir: PathBuf,
    pub active_dir: PathBuf,
    pub completed_dir: PathBuf,
    pub failed_dir: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AgentConfig {
    pub spawn_script: PathBuf,
    pub default_model: String,
    pub component_timeout_secs: u64,
}

//Expand ${VAR} patterns in a path using env vars.
//Unknown vars are kept as literal ${VAR}.
fn expand_path(path: PathBuf) -> PathBuf {
    let s = path.to_string_lossy();
    if !s.contains("${") {
        return path;
    }
    let re = Regex::new(r"\$\{([^}]+)\}").expect("valid regex");
    let expanded = re.replace_all(&s, |caps: &regex::Captures| {
        let var = &caps[1];
        std::env::var(var).unwrap_or_else(|_| format!("${{{}}}", var))
    });
    PathBuf::from(expanded.into_owned())
}

//Apply env var override if set, otherwise keep TOML value
fn env_override(toml_val: PathBuf, env_key: &str) -> PathBuf {
    std::env::var(env_key)
        .ok()
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or(toml_val)
}

//Build PlansConfig from env vars alone (no TOML section)
fn plans_from_env() -> Option<PlansConfig> {
    let drafts = std::env::var("PULSAR_PLANS_DRAFTS_DIR").ok()?;
    let active = std::env::var("PULSAR_PLANS_ACTIVE_DIR").ok()?;
    let completed = std::env::var("PULSAR_PLANS_COMPLETED_DIR").ok()?;
    let failed = std::env::var("PULSAR_PLANS_FAILED_DIR").ok()?;
    Some(PlansConfig {
        drafts_dir: PathBuf::from(drafts),
        active_dir: PathBuf::from(active),
        completed_dir: PathBuf::from(completed),
        failed_dir: PathBuf::from(failed),
    })
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read config {}: {}", path.display(), e))?;
        let mut config: Config = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse config: {}", e))?;

        //Expand ${VAR} patterns in all PathBuf fields
        config.scheduler.pipeline_ready_dir = expand_path(config.scheduler.pipeline_ready_dir);
        config.scheduler.pipeline_processing_dir = expand_path(config.scheduler.pipeline_processing_dir);
        config.scheduler.pipeline_completed_dir = expand_path(config.scheduler.pipeline_completed_dir);
        config.scheduler.task_queue_dir = expand_path(config.scheduler.task_queue_dir);
        config.scheduler.lock_file = expand_path(config.scheduler.lock_file);
        config.agent.spawn_script = expand_path(config.agent.spawn_script);
        if let Some(ref mut p) = config.plans {
            p.drafts_dir = expand_path(std::mem::take(&mut p.drafts_dir));
            p.active_dir = expand_path(std::mem::take(&mut p.active_dir));
            p.completed_dir = expand_path(std::mem::take(&mut p.completed_dir));
            p.failed_dir = expand_path(std::mem::take(&mut p.failed_dir));
        }

        //Resolve plans config: env vars override TOML, or build from env if TOML absent
        config.plans = match config.plans {
            Some(p) => Some(PlansConfig {
                drafts_dir: env_override(p.drafts_dir, "PULSAR_PLANS_DRAFTS_DIR"),
                active_dir: env_override(p.active_dir, "PULSAR_PLANS_ACTIVE_DIR"),
                completed_dir: env_override(p.completed_dir, "PULSAR_PLANS_COMPLETED_DIR"),
                failed_dir: env_override(p.failed_dir, "PULSAR_PLANS_FAILED_DIR"),
            }),
            None => plans_from_env(),
        };

        Ok(config)
    }

    //Convenience accessor — returns plans config or error
    pub fn plans(&self) -> anyhow::Result<&PlansConfig> {
        self.plans.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "no [plans] section in config and PULSAR_PLANS_*_DIR env vars not set"
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::io::Write;

    fn minimal_toml() -> &'static str {
        r#"
[scheduler]
interval_secs = 60
pipeline_ready_dir = "/tmp/ready"
pipeline_processing_dir = "/tmp/processing"
pipeline_completed_dir = "/tmp/completed"
task_queue_dir = "/tmp/tasks"
lock_file = "/tmp/lock"

[agent]
spawn_script = "/dev/null"
default_model = "sonnet"
component_timeout_secs = 30
"#
    }

    fn toml_with_plans() -> String {
        format!(
            r#"{}
[plans]
drafts_dir = "/toml/drafts"
active_dir = "/toml/active"
completed_dir = "/toml/completed"
failed_dir = "/toml/failed"
"#,
            minimal_toml()
        )
    }

    fn write_toml(dir: &Path, content: &str) -> PathBuf {
        let path = dir.join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    #[test]
    #[serial]
    fn test_config_loads_with_plans_section() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_toml(tmp.path(), &toml_with_plans());

        //Clear env vars so TOML values are used
        std::env::remove_var("PULSAR_PLANS_DRAFTS_DIR");
        std::env::remove_var("PULSAR_PLANS_ACTIVE_DIR");
        std::env::remove_var("PULSAR_PLANS_COMPLETED_DIR");
        std::env::remove_var("PULSAR_PLANS_FAILED_DIR");

        let config = Config::load(&path).unwrap();
        let plans = config.plans().unwrap();
        assert_eq!(plans.drafts_dir, PathBuf::from("/toml/drafts"));
        assert_eq!(plans.active_dir, PathBuf::from("/toml/active"));
        assert_eq!(plans.completed_dir, PathBuf::from("/toml/completed"));
        assert_eq!(plans.failed_dir, PathBuf::from("/toml/failed"));
    }

    #[test]
    #[serial]
    fn test_config_loads_without_plans_section() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_toml(tmp.path(), minimal_toml());

        //Clear env vars
        std::env::remove_var("PULSAR_PLANS_DRAFTS_DIR");
        std::env::remove_var("PULSAR_PLANS_ACTIVE_DIR");
        std::env::remove_var("PULSAR_PLANS_COMPLETED_DIR");
        std::env::remove_var("PULSAR_PLANS_FAILED_DIR");

        let config = Config::load(&path).unwrap();
        assert!(config.plans.is_none());
        assert!(config.plans().is_err());
    }

    #[test]
    #[serial]
    fn test_env_vars_override_toml_plans() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_toml(tmp.path(), &toml_with_plans());

        std::env::set_var("PULSAR_PLANS_DRAFTS_DIR", "/env/drafts");
        std::env::set_var("PULSAR_PLANS_ACTIVE_DIR", "/env/active");
        std::env::set_var("PULSAR_PLANS_COMPLETED_DIR", "/env/completed");
        std::env::set_var("PULSAR_PLANS_FAILED_DIR", "/env/failed");

        let config = Config::load(&path).unwrap();
        let plans = config.plans().unwrap();
        assert_eq!(plans.drafts_dir, PathBuf::from("/env/drafts"));
        assert_eq!(plans.active_dir, PathBuf::from("/env/active"));
        assert_eq!(plans.completed_dir, PathBuf::from("/env/completed"));
        assert_eq!(plans.failed_dir, PathBuf::from("/env/failed"));

        //Cleanup
        std::env::remove_var("PULSAR_PLANS_DRAFTS_DIR");
        std::env::remove_var("PULSAR_PLANS_ACTIVE_DIR");
        std::env::remove_var("PULSAR_PLANS_COMPLETED_DIR");
        std::env::remove_var("PULSAR_PLANS_FAILED_DIR");
    }

    #[test]
    #[serial]
    fn test_env_vars_provide_plans_when_toml_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_toml(tmp.path(), minimal_toml());

        std::env::set_var("PULSAR_PLANS_DRAFTS_DIR", "/env/drafts");
        std::env::set_var("PULSAR_PLANS_ACTIVE_DIR", "/env/active");
        std::env::set_var("PULSAR_PLANS_COMPLETED_DIR", "/env/completed");
        std::env::set_var("PULSAR_PLANS_FAILED_DIR", "/env/failed");

        let config = Config::load(&path).unwrap();
        let plans = config.plans().unwrap();
        assert_eq!(plans.drafts_dir, PathBuf::from("/env/drafts"));
        assert_eq!(plans.active_dir, PathBuf::from("/env/active"));

        //Cleanup
        std::env::remove_var("PULSAR_PLANS_DRAFTS_DIR");
        std::env::remove_var("PULSAR_PLANS_ACTIVE_DIR");
        std::env::remove_var("PULSAR_PLANS_COMPLETED_DIR");
        std::env::remove_var("PULSAR_PLANS_FAILED_DIR");
    }

    #[test]
    #[serial]
    fn test_default_toml_loads() {
        //Verify the actual config/default.toml loads without error
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/default.toml");
        if path.exists() {
            std::env::remove_var("PULSAR_PLANS_DRAFTS_DIR");
            std::env::remove_var("PULSAR_PLANS_ACTIVE_DIR");
            std::env::remove_var("PULSAR_PLANS_COMPLETED_DIR");
            std::env::remove_var("PULSAR_PLANS_FAILED_DIR");

            let config = Config::load(&path).unwrap();
            let plans = config.plans().unwrap();
            assert!(plans.drafts_dir.to_str().unwrap().contains("drafts"));
            assert!(plans.active_dir.to_str().unwrap().contains("active"));
        }
    }

    //--- expand_path unit tests ---
    //These tests use HOME (always set) and a unique nonexistent var name,
    //so they're safe to run multi-threaded without env var races.

    #[test]
    fn test_expand_path_known_var() {
        let home = std::env::var("HOME").unwrap();
        let result = expand_path(PathBuf::from("${HOME}/foo"));
        assert_eq!(result, PathBuf::from(format!("{}/foo", home)));
    }

    #[test]
    fn test_expand_path_unknown_var() {
        let result = expand_path(PathBuf::from("${NONEXISTENT_VAR_12345}/foo"));
        assert_eq!(result, PathBuf::from("${NONEXISTENT_VAR_12345}/foo"));
    }

    #[test]
    fn test_expand_path_mixed() {
        let home = std::env::var("HOME").unwrap();
        let result = expand_path(PathBuf::from("/static/${HOME}/more"));
        assert_eq!(result, PathBuf::from(format!("/static/{}/more", home)));
    }

    #[test]
    fn test_expand_path_no_var() {
        let result = expand_path(PathBuf::from("/plain/path"));
        assert_eq!(result, PathBuf::from("/plain/path"));
    }

    #[test]
    fn test_expand_path_empty_var_value() {
        //Use a unique var name to avoid races
        let var_name = format!("_PULSAR_TEST_EMPTY_{}", std::process::id());
        std::env::set_var(&var_name, "");
        let result = expand_path(PathBuf::from(format!("${{{}}}/foo", var_name)));
        assert_eq!(result, PathBuf::from("/foo"));
        std::env::remove_var(&var_name);
    }
}
