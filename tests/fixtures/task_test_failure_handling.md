# Test Failure Handling

**Task ID:** task_20260322_200100_fail
**Scheduler:** pulsar-relay

## Components

### Component 1: Succeed first
**Status:** PENDING
**Agent:** bash-scripting-expert

Create /tmp/pulsar-test-fail/success.txt with "Component 1 OK".

### Component 2: Designed to fail
**Status:** PENDING
**Agent:** nonexistent-agent-that-will-fail

Read from /nonexistent/path/that/does/not/exist.txt and parse its JSON contents. This component is intentionally designed to fail.

### Component 3: Should still be reachable
**Status:** PENDING
**Agent:** bash-scripting-expert

Create /tmp/pulsar-test-fail/step3.txt with "Component 3 reached despite Component 2 failure".
