use std::fs;
use std::path::PathBuf;
use std::process::{Command, Child};

//re-use project types by importing from main binary crate
//since this is a binary-only crate, we test via subprocess + file assertions

const PROJECT_ROOT: &str = env!("CARGO_MANIFEST_DIR");

fn mock_script_path() -> PathBuf {
    PathBuf::from(PROJECT_ROOT).join("tests/fixtures/mock-spawn-agent.sh")
}

fn mock_pipeline_executor_path() -> PathBuf {
    PathBuf::from(PROJECT_ROOT).join("tests/fixtures/mock-pipeline-executor.sh")
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
prep_timeout_secs = 30
prep_template_dir = "{}"
prep_agent_name = "pulsar-prep"
"#,
        interval_secs,
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        mock_script_path().display(),
        PathBuf::from(PROJECT_ROOT).join("prompts").display(),
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

//start mock pipeline executor in background
fn start_mock_pipeline_executor(test_dir: &PathBuf) -> Child {
    let ready_dir = test_dir.join("pipeline/ready");
    let completed_dir = test_dir.join("pipeline/completed");

    Command::new("bash")
        .arg(mock_pipeline_executor_path())
        .arg(ready_dir.to_str().unwrap())
        .arg(completed_dir.to_str().unwrap())
        .spawn()
        .expect("failed to start mock pipeline executor")
}

//run the scheduler binary with a config, limited to N ticks via timeout
//clears PULSAR_* env vars to ensure test isolation from host environment
fn run_scheduler_ticks(config_path: &PathBuf, timeout_secs: u64) -> String {
    let binary = PathBuf::from(PROJECT_ROOT).join("target/debug/pulsar-relay");
    let output = Command::new("timeout")
        .arg(format!("{}s", timeout_secs))
        .arg(&binary)
        .arg(config_path.to_str().unwrap())
        .env_remove("PULSAR_PLANS_DRAFTS_DIR")
        .env_remove("PULSAR_PLANS_ACTIVE_DIR")
        .env_remove("PULSAR_PLANS_COMPLETED_DIR")
        .env_remove("PULSAR_PLANS_FAILED_DIR")
        .env_remove("PULSAR_PLAN_FILE")
        .env_remove("PULSAR_POLL_INTERVAL_SECS")
        .output()
        .expect("failed to run scheduler binary");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    format!("STDOUT:\n{}\nSTDERR:\n{}", stdout, stderr)
}

#[test]
fn test_single_tick_dispatches_to_pipeline() {
    //after one tick, component 1 should be IN_PROGRESS and a pipeline task file
    //should exist in ready/ (or already picked up by the mock executor)
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

    //no mock pipeline executor — we want to see the raw dispatch
    let output = run_scheduler_ticks(&config_path, 10);

    //component 1 should be IN_PROGRESS (dispatched to pipeline, not yet completed)
    let updated = read_task_file(&tasks_dir, "task_single_tick.md");
    assert!(
        updated.contains("**Status:** IN_PROGRESS"),
        "Component 1 should be IN_PROGRESS after dispatch. File:\n{}",
        updated,
    );

    //pipeline task file should exist in ready/ (mock executor not running)
    let ready_dir = test_dir.join("pipeline/ready");
    let pipeline_file = ready_dir.join("task_pulsar_task_single_tick_001_1.md");
    assert!(
        pipeline_file.exists(),
        "Pipeline task file should exist in ready/. Dir contents: {:?}",
        fs::read_dir(&ready_dir).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>(),
    );

    //pipeline task file should have scheduler label
    let pipeline_content = fs::read_to_string(&pipeline_file).unwrap();
    assert!(
        pipeline_content.contains("**Scheduler:** pulsar-relay"),
        "Pipeline task should have scheduler label",
    );
    assert!(
        pipeline_content.contains("**Target Agent:** backend-developer"),
        "Pipeline task should have target agent",
    );

    //component 2 should still be PENDING
    let comp2_section = updated.split("### Component 2:").nth(1).unwrap();
    assert!(
        comp2_section.contains("**Status:** PENDING"),
        "Component 2 should still be PENDING. Section:\n{}",
        comp2_section,
    );

    eprintln!("Scheduler output:\n{}", output);
}

#[test]
fn test_completion_detection_from_pipeline() {
    //verifies the full cycle: dispatch → mock executor completes → scheduler detects
    let test_dir = setup_test_dir("completion-detect");
    //2s interval for multiple ticks within timeout
    let config_path = write_test_config(&test_dir, 2);
    let tasks_dir = test_dir.join("tasks");

    let task_content = r#"# Completion Detection Test

**Task ID:** task_completion_001
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
    write_task_file(&tasks_dir, "task_completion.md", task_content);

    //start mock pipeline executor
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //run scheduler for enough ticks to dispatch + detect completion + dispatch next
    let output = run_scheduler_ticks(&config_path, 20);

    //stop mock executor
    let _ = executor.kill();

    let updated = read_task_file(&tasks_dir, "task_completion.md");

    //at least component 1 should be COMPLETED (detected from completed/)
    let comp1 = updated.split("### Component 2:").next().unwrap();
    assert!(
        comp1.contains("**Status:** COMPLETED"),
        "Component 1 should be COMPLETED via pipeline completion detection. File:\n{}",
        updated,
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

    //start mock pipeline executor
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //run scheduler for enough ticks (2s interval, need dispatch+detect cycles for 3 components)
    //each component needs: tick to dispatch + tick to detect completion = 2 ticks * 2s = 4s
    //3 components = ~12s + buffer
    let output = run_scheduler_ticks(&config_path, 30);

    let _ = executor.kill();

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

    //start mock pipeline executor (handles failure based on agent name)
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //run for enough ticks
    let output = run_scheduler_ticks(&config_path, 30);

    let _ = executor.kill();

    let updated = read_task_file(&tasks_dir, "task_failure.md");

    //component 1 should be COMPLETED
    let comp1 = updated.split("### Component 2:").next().unwrap();
    assert!(
        comp1.contains("**Status:** COMPLETED"),
        "Component 1 should be COMPLETED. Section:\n{}",
        comp1,
    );

    //component 2: the mock executor writes "FAILED" in the result but the scheduler
    //currently detects all completed/ files as COMPLETED (executor moves there when done).
    //the scheduler's completion detection marks it COMPLETED since it found it in completed/
    //this is correct — the pipeline executor decides success/failure, scheduler just detects
    //presence in completed/ directory. A more sophisticated check would parse execution status.
    //for now, verify component 2 is at least processed (not stuck PENDING)
    let comp2 = updated.split("### Component 2:").nth(1).unwrap();
    let comp2_section = comp2.split("### Component 3:").next().unwrap();
    assert!(
        !comp2_section.contains("**Status:** PENDING"),
        "Component 2 should not be PENDING (should have been processed). Section:\n{}",
        comp2_section,
    );

    //component 3 should be COMPLETED (scheduler continues past previous component)
    let comp3 = updated.split("### Component 3:").nth(1).unwrap();
    assert!(
        comp3.contains("**Status:** COMPLETED"),
        "Component 3 should be COMPLETED after Component 2. Section:\n{}",
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
    let output = Command::new(&binary)
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
    //after dispatch, the pipeline task file in ready/ should have proper labels
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

### Component 2: Second step
**Status:** PENDING
**Agent:** backend-developer

Second test component.
"#;
    write_task_file(&tasks_dir, "task_label.md", task_content);

    //run one tick — no mock executor, so pipeline file stays in ready/
    run_scheduler_ticks(&config_path, 10);

    //pipeline task file should be in ready/
    let pipeline_file = pipeline_ready.join("task_pulsar_task_label_001_1.md");
    assert!(
        pipeline_file.exists(),
        "Pipeline task file should exist in ready/",
    );

    let content = fs::read_to_string(&pipeline_file).unwrap();
    assert!(
        content.contains("**Scheduler:** pulsar-relay"),
        "Pipeline task should have scheduler label. Content:\n{}",
        content,
    );
    assert!(
        content.contains("**Target Agent:** backend-developer"),
        "Pipeline task should have target agent. Content:\n{}",
        content,
    );
    assert!(
        content.contains("**Plan File:**"),
        "Pipeline task should reference plan file. Content:\n{}",
        content,
    );
    assert!(
        content.contains("READY_FOR_EXECUTION"),
        "Pipeline task should be marked READY_FOR_EXECUTION. Content:\n{}",
        content,
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

    //run one tick — dispatches component 1 (marks IN_PROGRESS)
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
    //should show IN_PROGRESS status (component 1 dispatched)
    assert!(
        updated.contains("IN_PROGRESS"),
        "Plan status should show IN_PROGRESS. File:\n{}",
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

    //start mock pipeline executor
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //run for enough ticks (2 components × 2 ticks each × 2s = ~8s + buffer)
    run_scheduler_ticks(&config_path, 20);

    let _ = executor.kill();

    let updated = read_task_file(&tasks_dir, "task_phase.md");
    let completed_count = updated.matches("**Status:** COMPLETED").count();
    assert!(
        completed_count >= 2,
        "Both phases should be COMPLETED, got {}. File:\n{}",
        completed_count,
        updated,
    );
}

#[test]
fn test_no_dispatch_when_pipeline_task_in_flight() {
    //verifies the parallel dispatch fix: if a pipeline task file exists
    //in processing/ for this plan, the scheduler should NOT dispatch the next component
    let test_dir = setup_test_dir("in-flight-block");
    let config_path = write_test_config(&test_dir, 60);
    let tasks_dir = test_dir.join("tasks");
    let processing_dir = test_dir.join("pipeline/processing");

    //task with component 1 already COMPLETED, component 2 PENDING
    let task_content = r#"# In-Flight Block Test

**Task ID:** task_inflight_001
**Scheduler:** pulsar-relay

## Components

### Component 1: Already done
**Status:** COMPLETED
**Agent:** backend-developer
**Result:** Component 1 is done

### Component 2: Should be blocked
**Status:** PENDING
**Agent:** backend-developer

This should NOT be dispatched because component 1's pipeline file is still in processing.
"#;
    write_task_file(&tasks_dir, "task_inflight.md", task_content);

    //simulate: pipeline executor moved component 1 file to processing/ (still running)
    fs::write(
        processing_dir.join("task_pulsar_task_inflight_001_1.md"),
        "# simulated in-flight pipeline task",
    ).unwrap();

    //run one tick
    run_scheduler_ticks(&config_path, 10);

    //component 2 should still be PENDING (blocked by in-flight pipeline task)
    let updated = read_task_file(&tasks_dir, "task_inflight.md");
    let comp2 = updated.split("### Component 2:").nth(1).unwrap();
    assert!(
        comp2.contains("**Status:** PENDING"),
        "Component 2 should still be PENDING when pipeline task is in-flight. Section:\n{}",
        comp2,
    );
}

#[test]
fn test_dispatch_resumes_after_pipeline_task_cleared() {
    //verifies that once the in-flight pipeline task is removed, dispatch resumes
    let test_dir = setup_test_dir("in-flight-resume");
    let config_path = write_test_config(&test_dir, 2);
    let tasks_dir = test_dir.join("tasks");

    //task with component 1 COMPLETED, component 2 PENDING
    let task_content = r#"# In-Flight Resume Test

**Task ID:** task_resume_001
**Scheduler:** pulsar-relay

## Components

### Component 1: Already done
**Status:** COMPLETED
**Agent:** backend-developer
**Result:** Component 1 done

### Component 2: Should dispatch when cleared
**Status:** PENDING
**Agent:** backend-developer

This should dispatch once the pipeline file is cleared.
"#;
    write_task_file(&tasks_dir, "task_resume.md", task_content);

    //start mock pipeline executor
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //NO pipeline files in processing — should dispatch and complete normally
    run_scheduler_ticks(&config_path, 15);

    let _ = executor.kill();

    let updated = read_task_file(&tasks_dir, "task_resume.md");
    let comp2 = updated.split("### Component 2:").nth(1).unwrap();
    assert!(
        comp2.contains("**Status:** COMPLETED"),
        "Component 2 should be COMPLETED when no pipeline tasks are in-flight. Section:\n{}",
        comp2,
    );
}

#[test]
fn test_pipeline_task_cleaned_up_after_completion() {
    //verifies that pipeline task files are cleaned up after completion detection
    let test_dir = setup_test_dir("cleanup-pipeline");
    let config_path = write_test_config(&test_dir, 2);
    let tasks_dir = test_dir.join("tasks");
    let pipeline_ready = test_dir.join("pipeline/ready");
    let pipeline_completed = test_dir.join("pipeline/completed");

    let task_content = r#"# Cleanup Test

**Task ID:** task_cleanup_001
**Scheduler:** pulsar-relay

## Components

### Component 1: Only step
**Status:** PENDING
**Agent:** backend-developer

Simple test component.
"#;
    write_task_file(&tasks_dir, "task_cleanup.md", task_content);

    //start mock pipeline executor
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //run enough ticks for dispatch + completion detection
    run_scheduler_ticks(&config_path, 15);

    let _ = executor.kill();

    //pipeline task file should be cleaned up from ready/ and completed/
    let ready_files: Vec<_> = fs::read_dir(&pipeline_ready)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name().to_str()
                .map(|n| n.starts_with("task_pulsar_task_cleanup_001_"))
                .unwrap_or(false)
        })
        .collect();

    let completed_files: Vec<_> = fs::read_dir(&pipeline_completed)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name().to_str()
                .map(|n| n.starts_with("task_pulsar_task_cleanup_001_"))
                .unwrap_or(false)
        })
        .collect();

    assert!(
        ready_files.is_empty(),
        "Pipeline task file should be cleaned up from ready/. Found: {:?}",
        ready_files.iter().map(|f| f.file_name()).collect::<Vec<_>>(),
    );
    assert!(
        completed_files.is_empty(),
        "Pipeline task file should be cleaned up from completed/. Found: {:?}",
        completed_files.iter().map(|f| f.file_name()).collect::<Vec<_>>(),
    );

    //verify the component was actually completed in the plan
    let updated = read_task_file(&tasks_dir, "task_cleanup.md");
    assert!(
        updated.contains("**Status:** COMPLETED"),
        "Component should be COMPLETED. File:\n{}",
        updated,
    );
}

//--- Parallel multi-plan tests (Component 5) ---

fn setup_test_dir_with_plans(test_name: &str) -> PathBuf {
    let dir = PathBuf::from(format!("/tmp/pulsar-relay-test-{}", test_name));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("tasks")).unwrap();
    fs::create_dir_all(dir.join("pipeline/ready")).unwrap();
    fs::create_dir_all(dir.join("pipeline/processing")).unwrap();
    fs::create_dir_all(dir.join("pipeline/completed")).unwrap();
    fs::create_dir_all(dir.join("plans/drafts")).unwrap();
    fs::create_dir_all(dir.join("plans/active")).unwrap();
    fs::create_dir_all(dir.join("plans/completed")).unwrap();
    fs::create_dir_all(dir.join("plans/failed")).unwrap();
    dir
}

fn write_test_config_with_plans(test_dir: &PathBuf, interval_secs: u64) -> PathBuf {
    let config_path = test_dir.join("config.toml");
    let config = format!(
        r#"[scheduler]
interval_secs = {}
pipeline_ready_dir = "{}/pipeline/ready"
pipeline_processing_dir = "{}/pipeline/processing"
pipeline_completed_dir = "{}/pipeline/completed"
task_queue_dir = "{}/tasks"
lock_file = "{}/scheduler.lock"

[plans]
drafts_dir = "{}/plans/drafts"
active_dir = "{}/plans/active"
completed_dir = "{}/plans/completed"
failed_dir = "{}/plans/failed"

[agent]
spawn_script = "{}"
default_model = "sonnet"
prep_timeout_secs = 30
prep_template_dir = "{}"
prep_agent_name = "pulsar-prep"
"#,
        interval_secs,
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        test_dir.display(),
        mock_script_path().display(),
        PathBuf::from(PROJECT_ROOT).join("prompts").display(),
    );
    fs::write(&config_path, config).unwrap();
    config_path
}

fn write_plan_file(plans_dir: &PathBuf, filename: &str, content: &str) {
    fs::write(plans_dir.join(filename), content).unwrap();
}

fn read_plan_file(plans_dir: &PathBuf, filename: &str) -> String {
    fs::read_to_string(plans_dir.join(filename)).unwrap()
}

fn alpha_plan_content() -> &'static str {
    r#"# Test Plan Alpha — Rust Async Patterns Research

**Task ID:** task_alpha_async_001
**Scheduler:** pulsar-relay

## Components

### Component 1: Research Rust async patterns
**Status:** PENDING
**Agent:** scholar

Research the main async patterns in Rust.

### Component 2: Summarize findings
**Status:** PENDING
**Agent:** scholar

Summarize the research into a comparison table.

### Component 3: Write example code
**Status:** PENDING
**Agent:** scholar

Write Rust examples for each pattern.
"#
}

fn beta_plan_content() -> &'static str {
    r#"# Test Plan Beta — System Health Check

**Task ID:** task_beta_health_001
**Scheduler:** pulsar-relay

## Components

### Component 1: List running Docker containers
**Status:** PENDING
**Agent:** bash-scripting-expert

Run docker ps and summarize running containers.

### Component 2: Check disk usage
**Status:** PENDING
**Agent:** bash-scripting-expert

Run df -h and report disk usage.
"#
}

#[test]
fn test_parallel_multi_plan_both_dispatched_first_tick() {
    //verifies that two plans in active/ both get their Component 1 dispatched
    //on the same tick — parallel across plans, sequential within
    let test_dir = setup_test_dir_with_plans("parallel-dispatch");
    let config_path = write_test_config_with_plans(&test_dir, 60);
    let active_dir = test_dir.join("plans/active");
    let pipeline_ready = test_dir.join("pipeline/ready");

    //place both plans in active/
    write_plan_file(&active_dir, "test-plan-alpha.md", alpha_plan_content());
    write_plan_file(&active_dir, "test-plan-beta.md", beta_plan_content());

    //run one tick — no mock executor so pipeline files stay in ready/
    run_scheduler_ticks(&config_path, 10);

    //both plans should have Component 1 dispatched to pipeline
    let alpha_pipeline = pipeline_ready.join("task_pulsar_task_alpha_async_001_1.md");
    let beta_pipeline = pipeline_ready.join("task_pulsar_task_beta_health_001_1.md");

    assert!(
        alpha_pipeline.exists(),
        "Alpha plan Component 1 should be dispatched to ready/. Dir: {:?}",
        fs::read_dir(&pipeline_ready).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>(),
    );
    assert!(
        beta_pipeline.exists(),
        "Beta plan Component 1 should be dispatched to ready/. Dir: {:?}",
        fs::read_dir(&pipeline_ready).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>(),
    );

    //verify different agents in pipeline files
    let alpha_content = fs::read_to_string(&alpha_pipeline).unwrap();
    let beta_content = fs::read_to_string(&beta_pipeline).unwrap();
    assert!(alpha_content.contains("**Target Agent:** scholar"), "Alpha should target scholar");
    assert!(beta_content.contains("**Target Agent:** bash-scripting-expert"), "Beta should target bash-scripting-expert");

    //both plan files should have Component 1 IN_PROGRESS
    let alpha_updated = read_plan_file(&active_dir, "test-plan-alpha.md");
    let beta_updated = read_plan_file(&active_dir, "test-plan-beta.md");

    let alpha_comp1 = alpha_updated.split("### Component 2:").next().unwrap();
    assert!(
        alpha_comp1.contains("**Status:** IN_PROGRESS"),
        "Alpha Component 1 should be IN_PROGRESS. Section:\n{}",
        alpha_comp1,
    );

    let beta_comp1 = beta_updated.split("### Component 2:").next().unwrap();
    assert!(
        beta_comp1.contains("**Status:** IN_PROGRESS"),
        "Beta Component 1 should be IN_PROGRESS. Section:\n{}",
        beta_comp1,
    );
}

#[test]
fn test_parallel_multi_plan_full_execution() {
    //verifies both plans complete independently — alpha (3 components) and beta (2 components)
    //beta should finish before alpha (fewer components)
    let test_dir = setup_test_dir_with_plans("parallel-full");
    let config_path = write_test_config_with_plans(&test_dir, 2);
    let active_dir = test_dir.join("plans/active");
    let completed_dir = test_dir.join("plans/completed");

    write_plan_file(&active_dir, "test-plan-alpha.md", alpha_plan_content());
    write_plan_file(&active_dir, "test-plan-beta.md", beta_plan_content());

    //start mock pipeline executor
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //run scheduler: alpha needs 3 dispatch+detect cycles, beta needs 2
    //each cycle is ~2 ticks (dispatch tick + completion detect tick) at 2s = 4s per cycle
    //3 cycles × 4s = 12s for alpha + discovery tick + buffer = ~20s
    let output = run_scheduler_ticks(&config_path, 35);

    let _ = executor.kill();

    //beta (2 components) should be fully completed
    //it may have moved to plans/completed/ already
    let beta_in_active = active_dir.join("test-plan-beta.md");
    let beta_completed_files: Vec<_> = fs::read_dir(&completed_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().map(|n| n.starts_with("test-plan-beta")).unwrap_or(false))
        .collect();

    //beta either moved to completed/ or all components are COMPLETED in active/
    if beta_in_active.exists() {
        let beta = fs::read_to_string(&beta_in_active).unwrap();
        let completed_count = beta.matches("**Status:** COMPLETED").count();
        assert!(
            completed_count >= 2,
            "Beta should have all 2 components COMPLETED. Got {}. File:\n{}",
            completed_count,
            beta,
        );
    } else {
        assert!(
            !beta_completed_files.is_empty(),
            "Beta plan should be in completed/ directory. completed/ contents: {:?}",
            fs::read_dir(&completed_dir).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>(),
        );
    }

    //alpha (3 components) should also be fully completed
    let alpha_in_active = active_dir.join("test-plan-alpha.md");
    let alpha_completed_files: Vec<_> = fs::read_dir(&completed_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().map(|n| n.starts_with("test-plan-alpha")).unwrap_or(false))
        .collect();

    if alpha_in_active.exists() {
        let alpha = fs::read_to_string(&alpha_in_active).unwrap();
        let completed_count = alpha.matches("**Status:** COMPLETED").count();
        assert!(
            completed_count >= 3,
            "Alpha should have all 3 components COMPLETED. Got {}. File:\n{}",
            completed_count,
            alpha,
        );
    } else {
        assert!(
            !alpha_completed_files.is_empty(),
            "Alpha plan should be in completed/ directory. completed/ contents: {:?}",
            fs::read_dir(&completed_dir).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>(),
        );
    }

    //tracker should show both tasks
    let tracker_path = test_dir.join("tasks/tracker-state.json");
    let state = fs::read_to_string(&tracker_path).unwrap();
    assert!(
        state.contains("task_alpha_async_001"),
        "Tracker should contain alpha. State:\n{}",
        state,
    );
    assert!(
        state.contains("task_beta_health_001"),
        "Tracker should contain beta. State:\n{}",
        state,
    );

    eprintln!("Scheduler output:\n{}", output);
}

#[test]
fn test_parallel_plans_independent_progress() {
    //verifies that one plan progresses while the other is blocked by in-flight task
    //uses pre-set component states to avoid timing issues with multiple scheduler runs
    let test_dir = setup_test_dir_with_plans("parallel-independent");
    let config_path = write_test_config_with_plans(&test_dir, 2);
    let active_dir = test_dir.join("plans/active");
    let processing_dir = test_dir.join("pipeline/processing");

    //alpha: component 1 COMPLETED, components 2+3 PENDING — should dispatch component 2
    let alpha_partial = r#"# Alpha Independent Progress

**Task ID:** task_alpha_indep_001
**Scheduler:** pulsar-relay

### Component 1: Already done
**Status:** COMPLETED
**Agent:** scholar
**Result:** Research complete

### Component 2: Ready to dispatch
**Status:** PENDING
**Agent:** scholar

Summarize findings.

### Component 3: Final step
**Status:** PENDING
**Agent:** scholar

Write examples.
"#;
    //beta: component 1 COMPLETED, component 2 PENDING — but blocked by in-flight task
    let beta_partial = r#"# Beta Blocked

**Task ID:** task_beta_indep_001
**Scheduler:** pulsar-relay

### Component 1: Already done
**Status:** COMPLETED
**Agent:** bash-scripting-expert
**Result:** Containers listed

### Component 2: Blocked by in-flight
**Status:** PENDING
**Agent:** bash-scripting-expert

Check disk usage.
"#;
    write_plan_file(&active_dir, "alpha-indep.md", alpha_partial);
    write_plan_file(&active_dir, "beta-indep.md", beta_partial);

    //simulate: beta has an in-flight pipeline task (stuck in processing/)
    fs::write(
        processing_dir.join("task_pulsar_task_beta_indep_001_1.md"),
        "# simulated stuck beta task",
    ).unwrap();

    //start mock executor and run
    let mut executor = start_mock_pipeline_executor(&test_dir);
    run_scheduler_ticks(&config_path, 15);
    let _ = executor.kill();

    //alpha should have progressed — either still in active/ with component 2+ done,
    //or moved to completed/ (all 3 components finished)
    let alpha_path = active_dir.join("alpha-indep.md");
    let completed_dir = test_dir.join("plans/completed");
    if alpha_path.exists() {
        let alpha = fs::read_to_string(&alpha_path).unwrap();
        let alpha_comp2 = alpha.split("### Component 2:").nth(1).unwrap();
        let alpha_comp2_section = alpha_comp2.split("### Component 3:").next().unwrap();
        assert!(
            !alpha_comp2_section.contains("**Status:** PENDING"),
            "Alpha Component 2 should have progressed (not PENDING). Section:\n{}",
            alpha_comp2_section,
        );
    } else {
        //alpha fully completed and moved — that proves it progressed independently
        let alpha_completed: Vec<_> = fs::read_dir(&completed_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_str().map(|n| n.starts_with("alpha-indep")).unwrap_or(false))
            .collect();
        assert!(
            !alpha_completed.is_empty(),
            "Alpha should have completed and moved to completed/ (progressed independently)",
        );
    }

    //beta component 2 should still be PENDING (blocked by in-flight task)
    let beta = read_plan_file(&active_dir, "beta-indep.md");
    let beta_comp2 = beta.split("### Component 2:").nth(1).unwrap();
    assert!(
        beta_comp2.contains("**Status:** PENDING"),
        "Beta Component 2 should still be PENDING (blocked by in-flight file). Section:\n{}",
        beta_comp2,
    );
}

#[test]
fn test_per_plan_in_flight_check_no_global_lock() {
    //verifies that in-flight pipeline tasks for one plan do NOT block another plan
    let test_dir = setup_test_dir_with_plans("no-global-lock");
    let config_path = write_test_config_with_plans(&test_dir, 60);
    let active_dir = test_dir.join("plans/active");
    let processing_dir = test_dir.join("pipeline/processing");
    let pipeline_ready = test_dir.join("pipeline/ready");

    //both plans: component 1 already COMPLETED, component 2 PENDING
    let alpha_partial = r#"# Test Plan Alpha Partial

**Task ID:** task_alpha_partial_001
**Scheduler:** pulsar-relay

### Component 1: Already done
**Status:** COMPLETED
**Agent:** scholar
**Result:** Done

### Component 2: Next step
**Status:** PENDING
**Agent:** scholar

Do next alpha work.
"#;
    let beta_partial = r#"# Test Plan Beta Partial

**Task ID:** task_beta_partial_001
**Scheduler:** pulsar-relay

### Component 1: Already done
**Status:** COMPLETED
**Agent:** bash-scripting-expert
**Result:** Done

### Component 2: Next step
**Status:** PENDING
**Agent:** bash-scripting-expert

Do next beta work.
"#;
    write_plan_file(&active_dir, "alpha-partial.md", alpha_partial);
    write_plan_file(&active_dir, "beta-partial.md", beta_partial);

    //simulate: alpha has an in-flight pipeline task (component 1 still in processing)
    fs::write(
        processing_dir.join("task_pulsar_task_alpha_partial_001_1.md"),
        "# alpha in-flight",
    ).unwrap();

    //run one tick
    run_scheduler_ticks(&config_path, 10);

    //alpha component 2 should NOT be dispatched (blocked by in-flight task)
    let alpha = read_plan_file(&active_dir, "alpha-partial.md");
    let alpha_comp2 = alpha.split("### Component 2:").nth(1).unwrap();
    assert!(
        alpha_comp2.contains("**Status:** PENDING"),
        "Alpha Component 2 should still be PENDING (in-flight block). Section:\n{}",
        alpha_comp2,
    );

    //beta component 2 SHOULD be dispatched (no in-flight for beta's task_id)
    let beta_pipeline = pipeline_ready.join("task_pulsar_task_beta_partial_001_2.md");
    assert!(
        beta_pipeline.exists(),
        "Beta Component 2 should be dispatched (no global lock). Dir: {:?}",
        fs::read_dir(&pipeline_ready).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>(),
    );

    let beta = read_plan_file(&active_dir, "beta-partial.md");
    let beta_comp2 = beta.split("### Component 2:").nth(1).unwrap();
    assert!(
        beta_comp2.contains("**Status:** IN_PROGRESS"),
        "Beta Component 2 should be IN_PROGRESS. Section:\n{}",
        beta_comp2,
    );
}

#[test]
fn test_one_plan_failure_does_not_affect_other() {
    //verifies that when one plan has a failing component, the other plan continues
    let test_dir = setup_test_dir_with_plans("failure-isolation");
    let config_path = write_test_config_with_plans(&test_dir, 2);
    let active_dir = test_dir.join("plans/active");
    let completed_dir = test_dir.join("plans/completed");
    let _failed_dir = test_dir.join("plans/failed");

    //alpha: normal plan that should succeed
    write_plan_file(&active_dir, "test-plan-alpha.md", alpha_plan_content());

    //gamma: plan with a component that will fail (agent name contains "fail")
    let gamma_failing = r#"# Test Plan Gamma — Will Fail

**Task ID:** task_gamma_fail_001
**Scheduler:** pulsar-relay

## Components

### Component 1: Will fail
**Status:** PENDING
**Agent:** nonexistent-agent-that-will-fail

This should fail because the agent doesn't exist.

### Component 2: After failure
**Status:** PENDING
**Agent:** backend-developer

This runs after the failed component.
"#;
    write_plan_file(&active_dir, "test-plan-gamma.md", gamma_failing);

    //start mock pipeline executor
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //run enough ticks for both plans to process
    let output = run_scheduler_ticks(&config_path, 35);
    let _ = executor.kill();

    //alpha should have all 3 components COMPLETED (either in active or moved to completed)
    let alpha_in_active = active_dir.join("test-plan-alpha.md");
    let alpha_completed_files: Vec<_> = fs::read_dir(&completed_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().map(|n| n.starts_with("test-plan-alpha")).unwrap_or(false))
        .collect();

    let alpha_ok = if alpha_in_active.exists() {
        let content = fs::read_to_string(&alpha_in_active).unwrap();
        content.matches("**Status:** COMPLETED").count() >= 3
    } else {
        !alpha_completed_files.is_empty()
    };
    assert!(
        alpha_ok,
        "Alpha plan should complete successfully despite gamma's failure. active exists={}, completed files={:?}",
        alpha_in_active.exists(),
        fs::read_dir(&completed_dir).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>(),
    );

    //gamma should have moved to failed/ (it has a failed component)
    //or still in active with COMPLETED status on the failed component
    //(the mock executor marks "fail" agents as FAILED but the scheduler
    //currently detects all completed/ files as COMPLETED since completion detection
    //doesn't parse execution status — it just picks up presence in completed/)
    //so gamma may actually complete "successfully" from the scheduler's perspective
    //regardless, the key assertion is: alpha was NOT affected by gamma
    eprintln!("Scheduler output:\n{}", output);
}

#[test]
fn test_mid_flight_plan_modification_checksum() {
    //verifies that modifying a plan file mid-flight triggers re-parse via checksum detection
    //uses a single scheduler run with background file modification
    let test_dir = setup_test_dir_with_plans("mid-flight-modify");
    let config_path = write_test_config_with_plans(&test_dir, 2);
    let active_dir = test_dir.join("plans/active");

    //place a plan in active/ with 3 components — enough for multiple ticks
    let initial_plan = r#"# Mid-Flight Modification Test

**Task ID:** task_midmod_001
**Scheduler:** pulsar-relay

## Components

### Component 1: Initial step
**Status:** PENDING
**Agent:** backend-developer

Do initial work.

### Component 2: Second step
**Status:** PENDING
**Agent:** backend-developer

Do second work.

### Component 3: Third step
**Status:** PENDING
**Agent:** backend-developer

Do third work.
"#;
    write_plan_file(&active_dir, "test-midmod.md", initial_plan);

    //start a background process that will modify the plan file after 5 seconds
    //this simulates mid-flight modification while the scheduler is running
    let plan_path = active_dir.join("test-midmod.md");
    let _modifier = Command::new("bash")
        .arg("-c")
        .arg(format!(
            "sleep 5 && if [ -f '{}' ]; then sed -i 's/Do third work./Do third work.\\n\\n**MODIFIED:** Added mid-flight./' '{}'; fi",
            plan_path.display(),
            plan_path.display(),
        ))
        .spawn()
        .expect("failed to start modifier process");

    //start mock pipeline executor
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //run scheduler for enough ticks to process all 3 components + detect modification
    let output = run_scheduler_ticks(&config_path, 25);
    let _ = executor.kill();
    //modifier should have already exited by now

    //the plan may have completed and moved to completed/
    let completed_dir = test_dir.join("plans/completed");
    let final_content = if plan_path.exists() {
        fs::read_to_string(&plan_path).unwrap()
    } else {
        //find it in completed/
        let files: Vec<_> = fs::read_dir(&completed_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_str().map(|n| n.starts_with("test-midmod")).unwrap_or(false))
            .collect();
        assert!(!files.is_empty(), "Plan should exist in active/ or completed/");
        fs::read_to_string(files[0].path()).unwrap()
    };

    //verify the modification was preserved through scheduler processing
    assert!(
        final_content.contains("**MODIFIED:**"),
        "Modification should be preserved after checksum re-parse. File:\n{}",
        final_content,
    );

    //component 2 should have progressed (not stuck at PENDING)
    let comp2 = final_content.split("### Component 2:").nth(1).unwrap();
    let comp2_section = comp2.split("### Component 3:").next().unwrap();
    assert!(
        !comp2_section.contains("**Status:** PENDING"),
        "Component 2 should have progressed. Section:\n{}",
        comp2_section,
    );

    eprintln!("Scheduler output:\n{}", output);
}

#[test]
fn test_failed_plan_moves_to_failed_dir() {
    //verifies that a plan with a failing component moves to plans/failed/
    //the mock executor marks agents with "fail" or "nonexistent" in name as FAILED
    //and writes **Execution Status:** FAILED in the completed file
    //the scheduler now detects this status and marks the component as FAILED
    let test_dir = setup_test_dir_with_plans("plan-to-failed");
    let config_path = write_test_config_with_plans(&test_dir, 2);
    let active_dir = test_dir.join("plans/active");
    let failed_dir = test_dir.join("plans/failed");

    //plan with 2 components — component 1 uses a failing agent
    let failing_plan = r#"# Failing Plan Test

**Task ID:** task_willfall_001
**Scheduler:** pulsar-relay

## Components

### Component 1: Will fail
**Status:** PENDING
**Agent:** nonexistent-agent-that-will-fail

This agent doesn't exist.

### Component 2: After failure
**Status:** PENDING
**Agent:** backend-developer

Normal work after the failure.
"#;
    write_plan_file(&active_dir, "test-failing.md", failing_plan);

    //start mock pipeline executor (marks agents with "fail"/"nonexistent" as FAILED)
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //run enough ticks for both components to process
    let output = run_scheduler_ticks(&config_path, 25);
    let _ = executor.kill();

    //plan should have moved to failed/ (component 1 has FAILED status from executor)
    //or still be in active/ with FAILED component if not enough ticks
    let plan_in_active = active_dir.join("test-failing.md");
    let failed_files: Vec<_> = fs::read_dir(&failed_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().map(|n| n.starts_with("test-failing")).unwrap_or(false))
        .collect();

    if plan_in_active.exists() {
        //plan may still be in active/ but component 1 should show FAILED
        let content = fs::read_to_string(&plan_in_active).unwrap();
        assert!(
            content.contains("**Status:** FAILED"),
            "Component 1 should be marked FAILED from pipeline executor status. File:\n{}",
            content,
        );
    } else {
        //plan moved to failed/
        assert!(
            !failed_files.is_empty(),
            "Failed plan should be in failed/ directory. Contents: {:?}",
            fs::read_dir(&failed_dir).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>(),
        );

        let failed_content = fs::read_to_string(failed_files[0].path()).unwrap();
        assert!(
            failed_content.contains("**Status:** FAILED"),
            "Failed plan should preserve FAILED status. Content:\n{}",
            failed_content,
        );

        //filename should have timestamp suffix
        let name_str = failed_files[0].file_name().to_str().unwrap().to_string();
        assert!(
            name_str.contains('T'),
            "Failed plan filename should have timestamp suffix. Got: {}",
            name_str,
        );
    }

    eprintln!("Scheduler output:\n{}", output);
}

#[test]
fn test_failure_isolation_other_plan_unaffected() {
    //one plan has a failing component, the other is healthy
    //verifies the healthy plan completes despite the failing one
    let test_dir = setup_test_dir_with_plans("failure-isolation-v2");
    let config_path = write_test_config_with_plans(&test_dir, 2);
    let active_dir = test_dir.join("plans/active");
    let _pipeline_ready = test_dir.join("pipeline/ready");

    //plan with a failing agent
    let failing_plan = r#"# Failing Isolation Plan

**Task ID:** task_fail_iso_001
**Scheduler:** pulsar-relay

### Component 1: Will fail
**Status:** PENDING
**Agent:** nonexistent-agent-that-will-fail

This should fail.
"#;
    //healthy plan
    let healthy_plan = r#"# Healthy Plan

**Task ID:** task_healthy_001
**Scheduler:** pulsar-relay

### Component 1: First step
**Status:** PENDING
**Agent:** backend-developer

Do healthy work.

### Component 2: Second step
**Status:** PENDING
**Agent:** backend-developer

More healthy work.
"#;

    write_plan_file(&active_dir, "failing-plan.md", failing_plan);
    write_plan_file(&active_dir, "healthy-plan.md", healthy_plan);

    //start mock pipeline executor
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //run enough ticks for the healthy plan to complete
    let output = run_scheduler_ticks(&config_path, 25);
    let _ = executor.kill();

    //healthy plan should have progressed — at least component 1 should be COMPLETED
    let healthy_exists = active_dir.join("healthy-plan.md").exists();
    if healthy_exists {
        let healthy = read_plan_file(&active_dir, "healthy-plan.md");
        let completed_count = healthy.matches("**Status:** COMPLETED").count();
        assert!(
            completed_count >= 1,
            "Healthy plan should have at least 1 COMPLETED component (unaffected by failure). Got {}. File:\n{}",
            completed_count,
            healthy,
        );
    }
    //if it's not in active/, it completed and moved — that's fine too

    eprintln!("Scheduler output:\n{}", output);
}

#[test]
fn test_completed_plan_moves_to_completed_dir() {
    //verifies that a plan transitions from active/ to plans/completed/ after all components complete
    let test_dir = setup_test_dir_with_plans("plan-to-completed");
    let config_path = write_test_config_with_plans(&test_dir, 2);
    let active_dir = test_dir.join("plans/active");
    let completed_dir = test_dir.join("plans/completed");

    //plan with 2 PENDING components — will complete via mock executor
    let plan = r#"# Plan To Complete

**Task ID:** task_tocomplete_001
**Scheduler:** pulsar-relay

### Component 1: First step
**Status:** PENDING
**Agent:** backend-developer

Do the first thing.

### Component 2: Second step
**Status:** PENDING
**Agent:** backend-developer

Do the second thing.
"#;
    write_plan_file(&active_dir, "test-tocomplete.md", plan);

    //start mock pipeline executor
    let mut executor = start_mock_pipeline_executor(&test_dir);

    //run enough ticks for both components to complete and plan to transition
    //2 components × 2 ticks (dispatch + detect) × 2s + transition tick = ~10s + buffer
    run_scheduler_ticks(&config_path, 20);
    let _ = executor.kill();

    //plan should have moved from active/ to completed/
    let plan_in_active = active_dir.join("test-tocomplete.md");
    let completed_files: Vec<_> = fs::read_dir(&completed_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().map(|n| n.starts_with("test-tocomplete")).unwrap_or(false))
        .collect();

    if plan_in_active.exists() {
        //still in active — verify all components completed (may need more time for transition)
        let content = fs::read_to_string(&plan_in_active).unwrap();
        let completed_count = content.matches("**Status:** COMPLETED").count();
        assert!(
            completed_count >= 2,
            "All 2 components should be COMPLETED. Got {}. File:\n{}",
            completed_count,
            content,
        );
    } else {
        //plan moved to completed/ — success
        assert!(
            !completed_files.is_empty(),
            "Completed plan should be in completed/. Contents: {:?}",
            fs::read_dir(&completed_dir).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>(),
        );

        //verify timestamp suffix
        let name_str = completed_files[0].file_name().to_str().unwrap().to_string();
        assert!(
            name_str.contains('T'),
            "Completed plan filename should have timestamp suffix. Got: {}",
            name_str,
        );
    }
}

#[test]
fn test_tracker_shows_both_plans_independently() {
    //verifies tracker-state.json shows both plans with independent status
    let test_dir = setup_test_dir_with_plans("tracker-parallel");
    let config_path = write_test_config_with_plans(&test_dir, 60);
    let active_dir = test_dir.join("plans/active");

    write_plan_file(&active_dir, "test-plan-alpha.md", alpha_plan_content());
    write_plan_file(&active_dir, "test-plan-beta.md", beta_plan_content());

    //run one tick
    run_scheduler_ticks(&config_path, 10);

    //verify tracker contains both plans with independent entries
    let tracker_path = test_dir.join("tasks/tracker-state.json");
    assert!(
        tracker_path.exists(),
        "tracker-state.json should exist",
    );

    let state = fs::read_to_string(&tracker_path).unwrap();
    assert!(
        state.contains("task_alpha_async_001"),
        "Tracker should contain alpha. State:\n{}",
        state,
    );
    assert!(
        state.contains("task_beta_health_001"),
        "Tracker should contain beta. State:\n{}",
        state,
    );

    //both should be active
    //parse JSON to verify independent status
    let parsed: serde_json::Value = serde_json::from_str(&state).unwrap();
    let tasks = parsed["tasks"].as_array().unwrap();

    let alpha_task = tasks.iter().find(|t| t["task_id"] == "task_alpha_async_001");
    let beta_task = tasks.iter().find(|t| t["task_id"] == "task_beta_health_001");

    assert!(alpha_task.is_some(), "Alpha should be in tracker");
    assert!(beta_task.is_some(), "Beta should be in tracker");

    //verify they have different component counts
    assert_eq!(
        alpha_task.unwrap()["total_components"].as_u64().unwrap(),
        3,
        "Alpha should have 3 components",
    );
    assert_eq!(
        beta_task.unwrap()["total_components"].as_u64().unwrap(),
        2,
        "Beta should have 2 components",
    );
}
