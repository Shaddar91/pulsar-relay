# Test Echo Pipeline

**Task ID:** task_20260322_200000_echo
**Scheduler:** pulsar-relay

## Components

### Component 1: Create workspace directory
**Status:** PENDING
**Agent:** bash-scripting-expert

Create a temporary directory at /tmp/pulsar-test-echo and write a file `step1.txt` containing "Component 1 completed at $(date)".

### Component 2: Read and extend
**Status:** PENDING
**Agent:** bash-scripting-expert

Read /tmp/pulsar-test-echo/step1.txt, then create step2.txt with the contents of step1.txt plus "Component 2 completed at $(date)".

### Component 3: Verify and summarize
**Status:** PENDING
**Agent:** bash-scripting-expert

List all files in /tmp/pulsar-test-echo/, read both step1.txt and step2.txt, and create a summary.txt with a count of files and their contents.
