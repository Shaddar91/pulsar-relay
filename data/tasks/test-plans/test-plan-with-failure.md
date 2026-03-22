# Test Plan: Failure Handling

**Task ID:** task_test_failure_plan_001
**Scheduler:** pulsar-relay

## Description
Test plan with 3 components where component 2 is designed to fail.
Verifies the scheduler marks failed components correctly and continues to component 3.

## Components

### Component 1: Succeed first
**Status:** PENDING
**Agent:** backend-developer

Echo "Component 1 succeeded". This should complete normally.

### Component 2: Deliberate failure
**Status:** PENDING
**Agent:** nonexistent-agent-that-will-fail

This component targets a nonexistent agent and should FAIL.
The scheduler should mark it FAILED and proceed to component 3.

### Component 3: Succeed after failure
**Status:** PENDING
**Agent:** backend-developer

Echo "Component 3 succeeded despite component 2 failure". Verifies scheduler continues past failures.
