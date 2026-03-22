mod config;
mod scheduler;
mod task_parser;
#[cfg(feature = "kafka")]
mod kafka;
mod agent;
mod tracker;

use std::path::PathBuf;
use tracing::{info, error};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    //init tracing with JSONL output
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pulsar_relay=info".into()),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config/default.toml"));

    info!(config = %config_path.display(), "loading configuration");

    let cfg = config::Config::load(&config_path)?;

    info!(
        interval_secs = cfg.scheduler.interval_secs,
        "pulsar-relay starting"
    );

    //ensure data directories exist
    std::fs::create_dir_all(&cfg.scheduler.task_queue_dir)?;

    let mut sched = scheduler::Scheduler::new(cfg);
    if let Err(e) = sched.run().await {
        error!(error = %e, "scheduler exited with error");
        std::process::exit(1);
    }

    Ok(())
}
