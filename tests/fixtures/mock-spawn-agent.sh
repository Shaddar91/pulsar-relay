#!/bin/bash
#mock prep agent for pulsar-relay integration tests
#simulates the generic prep agent: reads the plan, finds the IN_PROGRESS component
#(scheduler marks it before spawning), writes a pipeline task file to the
#ready dir from PULSAR_PIPELINE_READY_DIR env var, exits STATUS=ok.
#
#failure simulation: if --agent contains "fail" or "nonexistent", exits non-zero
#(preserved for existing tests that verify error rollback behavior).

set -eu

AGENT=""
MODEL=""
PROMPT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --agent) AGENT="$2"; shift 2 ;;
        --model) MODEL="$2"; shift 2 ;;
        --prompt) PROMPT="$2"; shift 2 ;;
        *) shift ;;
    esac
done

#failure simulation — preserved for tests that check rollback on agent error
if [[ "$AGENT" == *"fail"* ]] || [[ "$AGENT" == *"nonexistent"* ]]; then
    echo "ERROR: Agent '$AGENT' not found or execution failed" >&2
    exit 1
fi

#extract plan path from rendered prompt (template contains "**Plan file path:** <path>")
PLAN_PATH=$(printf '%s' "$PROMPT" | grep -oE '\*\*Plan file path:\*\*[[:space:]]+[^[:space:]]+' | sed -E 's/.*\*\*Plan file path:\*\*[[:space:]]+//' | head -1)

if [[ -z "${PLAN_PATH:-}" ]]; then
    echo "STATUS=failed missing plan_path in prompt"
    exit 0
fi

if [[ ! -f "$PLAN_PATH" ]]; then
    echo "STATUS=failed plan_file_not_found:$PLAN_PATH"
    exit 0
fi

#pipeline ready dir comes from scheduler-set env var (see agent::invoke_spawn_script)
READY_DIR="${PULSAR_PIPELINE_READY_DIR:-}"
if [[ -z "${READY_DIR:-}" ]]; then
    echo "STATUS=failed PULSAR_PIPELINE_READY_DIR not set"
    exit 0
fi

#find the IN_PROGRESS component — scheduler marks one before spawning
#awk walks line-by-line, tracking the most recent component header
COMPONENT_INFO=$(awk '
    /^### (Component|Phase) [0-9]+:/ {
        match($0, /[0-9]+/)
        current_idx = substr($0, RSTART, RLENGTH)
        sub(/^### (Component|Phase) [0-9]+:[[:space:]]*/, "")
        current_title = $0
    }
    /^\*\*Status:\*\*[[:space:]]+IN_PROGRESS/ {
        if (current_idx != "") {
            print current_idx "|" current_title
            exit
        }
    }
' "$PLAN_PATH")

if [[ -z "${COMPONENT_INFO:-}" ]]; then
    #no IN_PROGRESS component — either plan finished, or scheduler didn't mark one
    echo "STATUS=ok no_in_progress_component"
    exit 0
fi

COMPONENT_INDEX="${COMPONENT_INFO%%|*}"
COMPONENT_TITLE="${COMPONENT_INFO#*|}"

#extract Task ID (or derive from filename)
TASK_ID=$(grep -oE '\*\*Task ID:\*\*[[:space:]]+[^[:space:]]+' "$PLAN_PATH" | sed -E 's/.*\*\*Task ID:\*\*[[:space:]]+//' | head -1 || true)
if [[ -z "${TASK_ID:-}" ]]; then
    BASENAME=$(basename "$PLAN_PATH" .md)
    TASK_ID="task_${BASENAME}"
fi

#extract the component's body content (between its header and the next ### header)
#used for the pipeline task's Instructions section
COMPONENT_BODY=$(awk -v idx="$COMPONENT_INDEX" '
    BEGIN { capturing = 0 }
    /^### (Component|Phase) / {
        if (capturing) { exit }
        match($0, /[0-9]+/)
        cur = substr($0, RSTART, RLENGTH)
        if (cur == idx) { capturing = 1; next }
    }
    capturing { print }
' "$PLAN_PATH")

#resolve Target Agent — either the plan's **Agent:** field or default
AGENT_NAME=$(printf '%s' "$COMPONENT_BODY" | grep -oE '\*\*Agent:\*\*[[:space:]]+[^[:space:]]+' | sed -E 's/.*\*\*Agent:\*\*[[:space:]]+//' | head -1 || true)
if [[ -z "${AGENT_NAME:-}" ]]; then
    AGENT_NAME="backend-developer"
fi

#plan title (first "# " line)
PLAN_TITLE=$(grep -m1 '^# ' "$PLAN_PATH" | sed -E 's/^# //')

#build output path — matches the filename convention scheduler expects for
#has_in_flight_pipeline_tasks() and cleanup_pipeline_task()
SANITIZED_ID=$(printf '%s' "$TASK_ID" | tr ' /' '__')
PIPELINE_TASK_FILENAME="task_pulsar_${SANITIZED_ID}_${COMPONENT_INDEX}.md"
PIPELINE_TASK_PATH="${READY_DIR}/${PIPELINE_TASK_FILENAME}"

mkdir -p "$READY_DIR"

#write pipeline task file — format matches what the old write_pipeline_task
#produced, so existing tests that grep for specific labels still pass
CREATED_AT=$(date '+%Y-%m-%d %H:%M:%S')

{
    printf '# Task: %s — Component %s\n\n' "$PLAN_TITLE" "$COMPONENT_INDEX"
    printf '**Task ID:** %s_component_%s\n' "$SANITIZED_ID" "$COMPONENT_INDEX"
    printf '**Created:** %s\n' "$CREATED_AT"
    printf '**Scheduler:** pulsar-relay\n'
    printf '**Plan File:** %s\n' "$PLAN_PATH"
    printf '**Component:** %s\n' "$COMPONENT_INDEX"
    printf '**Priority:** Medium\n'
    printf '**Type:** code\n'
    printf '**Target Agent:** %s\n' "$AGENT_NAME"
    printf '**Agent Available:** Yes\n'
    printf '**Routing Confidence:** 95%%\n'
    printf '**Ready Status:** READY_FOR_EXECUTION\n\n'
    printf '## Summary\n'
    printf 'Component %s of plan "%s": %s\n\n' "$COMPONENT_INDEX" "$PLAN_TITLE" "$COMPONENT_TITLE"
    printf '## Instructions\n\n'
    printf '%s\n\n' "$COMPONENT_BODY"
    printf '## Dynamic Agent Config\n'
    printf '```json\n'
    printf '{\n  "agent": "%s",\n  "source": "agents/models/default/%s.json"\n}\n' "$AGENT_NAME" "$AGENT_NAME"
    printf '```\n'
} > "$PIPELINE_TASK_PATH"

echo "STATUS=ok dispatched ${SANITIZED_ID}_c${COMPONENT_INDEX}"
