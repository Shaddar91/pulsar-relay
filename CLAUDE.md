# Pulsar Relay — Sequential Task Scheduler

## Project Overview
Rust-based sequential task scheduler for the AI Task Automation pipeline. Processes large multi-component tasks one component at a time, backed by Kafka for queue durability.

## Location
- **Repo:** `Shaddar91/pulsar-relay` (private)
- **Local:** `/home/shaddar/Documents/workspace/personal/projects/pulsar-relay`
- **Forge:** `/home/shaddar/Documents/workspace/personal/projects/forge/projects/pulsar-relay/tasks/implementation-plan.md`

## MANDATORY Rules for Pipeline Agents

1. **ALWAYS read the Forge implementation plan FIRST** before starting any work:
   `/home/shaddar/Documents/workspace/personal/projects/forge/projects/pulsar-relay/tasks/implementation-plan.md`
   This contains current status, what's done, what's pending, and architecture decisions.

2. **ALWAYS update the Forge implementation plan AFTER completing work:**
   - Mark completed action items with `[x]`
   - Update the `Current Status` section with what was accomplished
   - Update `Last Updated` date
   - Move resolved blockers to strikethrough
   - Update phase status in the `Time Investment Required` table
   - Add any new blockers or findings discovered during work

3. **DO NOT duplicate work** — check what's already done in the plan before starting.

4. **DO NOT push to remote** — commit locally, user will review and push.

5. **Commit messages** must reference the phase being worked on (e.g., "Phase 3: Kafka integration — add producer/consumer tests").

## Tech Stack
- **Language:** Rust (edition 2021)
- **Queue:** Apache Kafka 3.7.0 (KRaft mode, optional compile-time feature `--features kafka`)
- **Config:** TOML (`config/default.toml`)
- **Logging:** JSONL via tracing + tracing-subscriber
- **Locking:** flock via fs2 crate
- **Containers:** Docker Compose (Kafka + Kafka UI on :8089)

## Architecture
```
src/
├── main.rs          # Entry point, tracing init
├── config.rs        # TOML config deserialization
├── scheduler.rs     # Core 15-min cron loop with flock
├── task_parser.rs   # Parse multi-component .md task files
├── agent.rs         # Spawn agents via spawn-agent-v2.sh
├── tracker.rs       # JSON-based task state tracking
└── kafka/
    ├── mod.rs
    ├── producer.rs  # Enqueue components to Kafka
    └── consumer.rs  # Poll completion results
```

## Integration Points
- **Spawn script:** `/home/shaddar/Documents/workspace/ai-task-automation/scripts/spawn-agent-v2.sh`
- **Agent JSONs:** `/home/shaddar/Documents/workspace/ai-task-automation/agents/models/default/`
- **Pipeline root:** `/home/shaddar/Documents/workspace/ai-task-automation/`

## Coding Preferences
- No space after `//` or `#` in comments
- JSONL format for logging
- Error handling: explicit, no unwrap() in production code

## Code Quality — STRICT

**Follow functional programming patterns:**
- Each logical concern gets its own function — small, composable, single-purpose
- No monolithic functions that do 10 things. If a function is doing more than one logical step, split it.
- Pure functions where possible — input in, output out, no side effects
- Side effects (IO, state mutation) isolated at the edges, not buried in business logic
- Use Rust's type system: enums for states, Result for errors, Option for absence
- Name functions by what they DO, not what they're part of (e.g. `parse_component_status()` not `helper()`)
- Each module (`scheduler.rs`, `task_parser.rs`, etc.) should expose a clean public API of composable functions
- No "random shit code" — every function should be testable in isolation

## Agent Execution Permissions

Agents are authorized to:
- Run `docker compose up -d` / `docker compose down` locally
- Run `cargo run`, `cargo run --features kafka`, `cargo test`
- Install deps (`sudo apt install cmake`, etc.)
- Test Kafka connectivity, produce/consume messages
- Iterate on problems — debug, fix, rebuild, retry
- When blocked: post specific blockers to the Forge plan, commit working code, exit. Next agent picks up in ~5 min.
