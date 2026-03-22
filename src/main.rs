mod config;
mod scheduler;
mod task_parser;
#[cfg(feature = "kafka")]
mod kafka;
mod agent;
mod tracker;
mod ingest;

use std::path::PathBuf;
use tracing::{info, error};

fn init_tracing() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pulsar_relay=info".into()),
        )
        .init();
}

fn parse_config_flag(args: &[String], default: &str) -> PathBuf {
    args.iter()
        .position(|a| a == "--config")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default))
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  pulsar-relay [config.toml]                         Run scheduler loop");
    eprintln!("  pulsar-relay ingest <path> [--config config.toml]  Ingest plan file or folder");
    eprintln!();
    eprintln!("Arguments:");
    eprintln!("  <path>    Path to a plan .md file or folder of plan files");
    eprintln!("  --config  Path to config file (default: config/default.toml)");
}

async fn run_scheduler(config_path: PathBuf) -> anyhow::Result<()> {
    info!(config = %config_path.display(), "loading configuration");
    let cfg = config::Config::load(&config_path)?;

    info!(
        interval_secs = cfg.scheduler.interval_secs,
        "pulsar-relay starting"
    );

    //ensure data directories exist
    std::fs::create_dir_all(&cfg.scheduler.task_queue_dir)?;

    let mut sched = scheduler::Scheduler::new(cfg);
    sched.run().await
}

async fn run_ingest(ingest_path: PathBuf, config_path: PathBuf) -> anyhow::Result<()> {
    info!(
        path = %ingest_path.display(),
        config = %config_path.display(),
        "ingest mode"
    );

    let cfg = config::Config::load(&config_path)?;
    std::fs::create_dir_all(&cfg.scheduler.task_queue_dir)?;

    let mut tracker = tracker::TaskTracker::new(cfg.scheduler.task_queue_dir.clone());
    let ids = ingest::ingest_path(&ingest_path, &cfg, &mut tracker)?;

    for id in &ids {
        info!(task_id = %id, "plan registered with scheduler");
    }

    eprintln!("Ingested {} plan(s). Scheduler will process them on next tick.", ids.len());
    for id in &ids {
        eprintln!("  - {}", id);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let args: Vec<String> = std::env::args().collect();

    //check for ingest subcommand
    if args.len() > 1 && args[1] == "ingest" {
        if args.len() < 3 || args[2] == "--help" || args[2] == "-h" {
            print_usage();
            std::process::exit(if args.len() < 3 { 1 } else { 0 });
        }

        let path = PathBuf::from(&args[2]);
        let config_path = parse_config_flag(&args, "config/default.toml");

        if let Err(e) = run_ingest(path, config_path).await {
            error!(error = %e, "ingest failed");
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
        return Ok(());
    }

    //check for help
    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        print_usage();
        return Ok(());
    }

    //scheduler mode
    let config_path = args.get(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config/default.toml"));

    if let Err(e) = run_scheduler(config_path).await {
        error!(error = %e, "scheduler exited with error");
        std::process::exit(1);
    }

    Ok(())
}
