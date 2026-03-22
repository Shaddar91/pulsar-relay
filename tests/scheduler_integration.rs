use std::fs;
use std::path::PathBuf;

//re-use project types by importing from main binary crate
//since this is a binary-only crate, we test via subprocess + file assertions

const PROJECT_ROOT: &str = env!("CARGO_MANIFEST_DIR");

fn mock_script_path() -> PathBuf {
    PathBuf::from(PROJECT_ROOT).join("tests/fixtures/mock-spawn-agent.sh")
}

fn setup_test_dir(test_name: &str) -> PathBuf {
    let dir = PathBuf::from(format!("/tmp/pulsar-relay-test-{}", test_name));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("tasks")).unwrap();
    fs::create_dir_all(dir.join("pipeline/ready")).unwrap();
    fs::create_dir_all(dir.join("pipeline/processing")).unwrap();
    fs::create_dir_all(dir.join("pipeline/completed")).unwrap();
    dir
}

fn write_test_config(test_dir: &PathBuf, interval_secs: u64) -> PathBuf {
    let config_path = test_dir.join("config.toml");
    let config = format!(
        r#"[scheduler]
interval_secs = {}
pipeline_ready_dir = "{}/pipeline/ready"
pipeline_processing_dir = "{}/pipeline/processing"
pipeline_completed_dir = "{}/pipeline/completed"
task_queue_dir = "{}/tasks"
lock_file = "{}/scheduler.lock"

[agent]
spawn_script = "{}"
default_model = "sonnet"
component_timeout_secs = 30
"#,
        interval_secs,
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        mock_script_path().display(),
    );
    fs::write(&config_path, config).unwrap();
    config_path
}

fn write_task_file(tasks_dir: &PathBuf, filename: &str, content: &str) {
    fs::write(tasks_dir.join(filename), content).unwrap();
}

fn read_task_file(tasks_dir: &PathBuf, filename: &str) -> String {
    fs::read_to_string(tasks_dir.join(filename)).unwrap()
}

//run the scheduler binary with a config, limited to N ticks via timeout
fn run_scheduler_ticks(config_path: &PathBuf, timeout_secs: u64) -> String {
    let binary = PathBuf::from(PROJECT_ROOT).join("target/debug/pulsar-relay");
    let output = std::process::Command::new("timeout")
        .arg(format!("{}s", timeout_secs))
        .arg(&binary)
        .arg(config_path.to_str().unwrap())
        .output()
        .expect("failed to run scheduler binary");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    format!("STDOUT:\n{}\nSTDERR:\n{}", stdout, stderr)
}

#[test]
fn test_single_tick_processes_first_component() {
    let test_dir = setup_test_dir("single-tick");
    //long interval so only 1 tick fires before timeout
    let config_path = write_test_config(&test_dir, 60);
    let tasks_dir = test_dir.join("tasks");

    let task_content = r#"# Single Tick Test

**Task ID:** task_single_tick_001
**Scheduler:** pulsar-relay

## Components

### Component 1: First step
**Status:** PENDING
**Agent:** backend-developer

Echo hello.

### Component 2: Second step
**Status:** PENDING
**Agent:** backend-developer

Echo world.
"#;
    write_task_file(&tasks_dir, "task_single_tick.md", task_content);

    //run scheduler for one tick (generous timeout for parallel test load)
    let output = run_scheduler_ticks(&config_path, 10);

    //debug: print scheduler output
    eprintln!("=== Scheduler output ===\n{}", output);

    //verify component 1 is COMPLETED with a result
    let updated = read_task_file(&tasks_dir, "task_single_tick.md");
    assert!(
        updated.contains("**Status:** COMPLETED"),
        "Component 1 should be COMPLETED. File:\n{}",
        updated,
    );
    assert!(
        updated.contains("**Result:**"),
        "Component 1 should have a Result. File:\n{}",
        updated,
    );
    //component 2 should still be PENDING (only one per tick)
    let comp2_section = updated.split("### Component 2:").nth(1).unwrap();
    assert!(
        comp2_section.contains("**Status:** PENDING"),
        "Component 2 should still be PENDING after first tick. Section:\n{}",
        comp2_section,
    );

    eprintln!("Scheduler output:\n{}", output);
}

#[test]
fn test_multiple_ticks_sequential_processing() {
    let test_dir = setup_test_dir("multi-tick");
    let config_path = write_test_config(&test_dir, 2);
    let tasks_dir = test_dir.join("tasks");

    let task_content = r#"# Multi Tick Test

**Task ID:** task_multi_tick_001
**Scheduler:** pulsar-relay

## Components

### Component 1: Step one
**Status:** PENDING
**Agent:** backend-developer

Do step one.

### Component 2: Step two
**Status:** PENDING
**Agent:** backend-developer

Do step two.

### Component 3: Step three
**Status:** PENDING
**Agent:** frontend-developer

Do step three.
"#;
    write_task_file(&tasks_dir, "task_multi_tick.md", task_content);

    //run scheduler for 3 ticks (generous timeout for parallel test load)
    let output = run_scheduler_ticks(&config_path, 30);

    let updated = read_task_file(&tasks_dir, "task_multi_tick.md");

    //all three components should be COMPLETED
    let completed_count = updated.matches("**Status:** COMPLETED").count();
    assert!(
        completed_count >= 3,
        "All 3 components should be COMPLETED, got {}. File:\n{}",
        completed_count,
        updated,
    );

    //verify results are present for all
    let result_count = updated.matches("**Result:**").count();
    assert!(
        result_count >= 3,
        "All 3 components should have Results, got {}. File:\n{}",
        result_count,
        updated,
    );

    //verify tracker state shows task as completed
    let tracker_path = tasks_dir.join("tracker-state.json");
    let state = fs::read_to_string(&tracker_path).unwrap();
    assert!(
        state.contains("\"status\":\"completed\"") || state.contains("\"status\": \"completed\""),
        "Tracker should mark task as completed. State:\n{}",
        state,
    );

    eprintln!("Scheduler output:\n{}", output);
}

#[test]
fn test_failure_handling_marks_failed() {
    let test_dir = setup_test_dir("failure");
    let config_path = write_test_config(&test_dir, 2);
    let tasks_dir = test_dir.join("tasks");

    let task_content = r#"# Failure Test

**Task ID:** task_failure_001
**Scheduler:** pulsar-relay

## Components

### Component 1: Will succeed
**Status:** PENDING
**Agent:** backend-developer

This should work fine.

### Component 2: Will fail
**Status:** PENDING
**Agent:** nonexistent-agent-that-will-fail

This should fail because the agent doesn't exist.

### Component 3: After failure
**Status:** PENDING
**Agent:** backend-developer

This should still be reachable on next tick.
"#;
    write_task_file(&tasks_dir, "task_failure.md", task_content);

    //run for 3+ ticks (generous timeout for parallel test load)
    let output = run_scheduler_ticks(&config_path, 30);

    let updated = read_task_file(&tasks_dir, "task_failure.md");

    //component 1 should be COMPLETED
    let comp1 = updated.split("### Component 2:").next().unwrap();
    assert!(
        comp1.contains("**Status:** COMPLETED"),
        "Component 1 should be COMPLETED. Section:\n{}",
        comp1,
    );

    //component 2 should be FAILED
    let comp2 = updated.split("### Component 2:").nth(1).unwrap();
    let comp2_section = comp2.split("### Component 3:").next().unwrap();
    assert!(
        comp2_section.contains("**Status:** FAILED"),
        "Component 2 should be FAILED. Section:\n{}",
        comp2_section,
    );

    //component 3 should be COMPLETED (scheduler continues past failure)
    let comp3 = updated.split("### Component 3:").nth(1).unwrap();
    assert!(
        comp3.contains("**Status:** COMPLETED"),
        "Component 3 should be COMPLETED despite Component 2 failure. Section:\n{}",
        comp3,
    );

    eprintln!("Scheduler output:\n{}", output);
}

#[test]
fn test_tracker_state_persists() {
    let test_dir = setup_test_dir("tracker");
    let config_path = write_test_config(&test_dir, 60);
    let tasks_dir = test_dir.join("tasks");

    let task_content = r#"# Tracker Test

**Task ID:** task_tracker_001
**Scheduler:** pulsar-relay

## Components

### Component 1: Only step
**Status:** PENDING
**Agent:** backend-developer

Just one component.
"#;
    write_task_file(&tasks_dir, "task_tracker.md", task_content);

    //run for one tick (generous timeout for CI load)
    run_scheduler_ticks(&config_path, 10);

    //verify tracker state file exists
    let tracker_path = tasks_dir.join("tracker-state.json");
    assert!(
        tracker_path.exists(),
        "tracker-state.json should exist at {}",
        tracker_path.display(),
    );

    let state = fs::read_to_string(&tracker_path).unwrap();
    assert!(
        state.contains("task_tracker_001"),
        "Tracker should contain task_tracker_001. State:\n{}",
        state,
    );
}

#[test]
fn test_already_finished_task_is_skipped() {
    let test_dir = setup_test_dir("skip-finished");
    let config_path = write_test_config(&test_dir, 60);
    let tasks_dir = test_dir.join("tasks");

    //task with all components already completed
    let task_content = r#"# Already Done

**Task ID:** task_already_done
**Scheduler:** pulsar-relay

## Components

### Component 1: Done
**Status:** COMPLETED
**Agent:** backend-developer
**Result:** Already finished

### Component 2: Also done
**Status:** COMPLETED
**Agent:** backend-developer
**Result:** Already finished too
"#;
    write_task_file(&tasks_dir, "task_already_done.md", task_content);

    //run one tick — should not modify the file
    let before = read_task_file(&tasks_dir, "task_already_done.md");
    run_scheduler_ticks(&config_path, 10);
    let after = read_task_file(&tasks_dir, "task_already_done.md");

    assert_eq!(
        before, after,
        "Already-finished task should not be modified",
    );
}

#[test]
fn test_ingest_cli_registers_plan() {
    let test_dir = setup_test_dir("ingest-cli");
    let config_path = write_test_config(&test_dir, 60);
    let tasks_dir = test_dir.join("tasks");

    //create a plan file outside the task queue
    let plan_dir = test_dir.join("plans");
    fs::create_dir_all(&plan_dir).unwrap();
    let plan_content = r#"# Ingested Plan

**Task ID:** task_ingest_cli_001
**Scheduler:** pulsar-relay

### Component 1: First
**Status:** PENDING
**Agent:** backend-developer

Do the first thing.

### Component 2: Second
**Status:** PENDING
**Agent:** backend-developer

Do the second thing.
"#;
    let plan_path = plan_dir.join("test-plan.md");
    fs::write(&plan_path, plan_content).unwrap();

    //run ingest CLI
    let binary = PathBuf::from(PROJECT_ROOT).join("target/debug/pulsar-relay");
    let output = std::process::Command::new(&binary)
        .arg("ingest")
        .arg(plan_path.to_str().unwrap())
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .output()
        .expect("failed to run ingest command");

    assert!(
        output.status.success(),
        "ingest should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );

    //verify tracker contains the plan
    let tracker_path = tasks_dir.join("tracker-state.json");
    assert!(
        tracker_path.exists(),
        "tracker-state.json should exist after ingest",
    );
    let state = fs::read_to_string(&tracker_path).unwrap();
    assert!(
        state.contains("task_ingest_cli_001"),
        "Tracker should contain ingested task. State:\n{}",
        state,
    );
}

#[test]
fn test_pipeline_task_has_scheduler_label() {
    let test_dir = setup_test_dir("pipeline-label");
    let config_path = write_test_config(&test_dir, 60);
    let tasks_dir = test_dir.join("tasks");
    let pipeline_ready = test_dir.join("pipeline/ready");

    let task_content = r#"# Pipeline Label Test

**Task ID:** task_label_001
**Scheduler:** pulsar-relay

## Components

### Component 1: First step
**Status:** PENDING
**Agent:** backend-developer

Test component.
"#;
    write_task_file(&tasks_dir, "task_label.md", task_content);

    //run scheduler — should dispatch and write pipeline task file
    run_scheduler_ticks(&config_path, 10);

    //verify pipeline task file exists with scheduler label
    let pipeline_files: Vec<_> = fs::read_dir(&pipeline_ready)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext == "md").unwrap_or(false))
        .collect();

    assert!(
        !pipeline_files.is_empty(),
        "Pipeline ready dir should contain task file(s). Dir: {}",
        pipeline_ready.display(),
    );

    let pipeline_content = fs::read_to_string(pipeline_files[0].path()).unwrap();
    assert!(
        pipeline_content.contains("**Scheduler:** pulsar-relay"),
        "Pipeline task file should contain scheduler label. Content:\n{}",
        pipeline_content,
    );
    assert!(
        pipeline_content.contains("**Target Agent:**"),
        "Pipeline task file should contain target agent. Content:\n{}",
        pipeline_content,
    );
}

#[test]
fn test_plan_header_updated_during_execution() {
    let test_dir = setup_test_dir("plan-header");
    let config_path = write_test_config(&test_dir, 60);
    let tasks_dir = test_dir.join("tasks");

    let task_content = r#"# Plan Header Test

**Task ID:** task_plan_header_001
**Scheduler:** pulsar-relay

### Component 1: Step one
**Status:** PENDING
**Agent:** backend-developer

Do step one.

### Component 2: Step two
**Status:** PENDING
**Agent:** backend-developer

Do step two.
"#;
    write_task_file(&tasks_dir, "task_plan_header.md", task_content);

    //run one tick — component 1 should complete
    run_scheduler_ticks(&config_path, 10);

    let updated = read_task_file(&tasks_dir, "task_plan_header.md");
    assert!(
        updated.contains("**Plan Status:**"),
        "Plan header should contain Plan Status. File:\n{}",
        updated,
    );
    assert!(
        updated.contains("**Progress:**"),
        "Plan header should contain Progress. File:\n{}",
        updated,
    );
    assert!(
        updated.contains("1 completed"),
        "Progress should show 1 completed. File:\n{}",
        updated,
    );
}

#[test]
fn test_phase_headers_parsed_correctly() {
    let test_dir = setup_test_dir("phase-headers");
    let config_path = write_test_config(&test_dir, 2);
    let tasks_dir = test_dir.join("tasks");

    let task_content = r#"# Phase Header Test

**Task ID:** task_phase_001
**Scheduler:** pulsar-relay

### Phase 1: Setup
**Status:** PENDING
**Agent:** backend-developer

Do setup work.

### Phase 2: Execute
**Status:** PENDING
**Agent:** backend-developer

Do execution work.
"#;
    write_task_file(&tasks_dir, "task_phase.md", task_content);

    //run for 2+ ticks (generous timeout for parallel test load)
    run_scheduler_ticks(&config_path, 20);

    let updated = read_task_file(&tasks_dir, "task_phase.md");
    let completed_count = updated.matches("**Status:** COMPLETED").count();
    assert!(
        completed_count >= 2,
        "Both phases should be COMPLETED, got {}. File:\n{}",
        completed_count,
        updated,
    );
}
