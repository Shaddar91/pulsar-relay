# Test Plan: Echo Pipeline

**Task ID:** task_test_echo_plan_001
**Scheduler:** pulsar-relay

## Description
Simple echo test plan with 3 components that just produce text output.
Used to verify the scheduler processes components sequentially and writes results back.

## Components

### Component 1: Echo greeting
**Status:** PENDING
**Agent:** backend-developer

Echo "Hello from component 1". This is a simple test — just produce output text.

### Component 2: Echo status
**Status:** PENDING
**Agent:** backend-developer

Echo "Component 2 reporting in". Check that component 1 has a result before proceeding.

### Component 3: Echo summary
**Status:** PENDING
**Agent:** backend-developer

Echo "All 3 components executed successfully". This is the final component.
