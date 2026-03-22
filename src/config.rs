use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct Config {
    pub scheduler: SchedulerConfig,
    #[serde(default)]
    pub kafka: Option<KafkaConfig>,
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
pub struct AgentConfig {
    pub spawn_script: PathBuf,
    pub default_model: String,
    pub component_timeout_secs: u64,
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read config {}: {}", path.display(), e))?;
        let config: Config = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse config: {}", e))?;
        Ok(config)
    }
}
