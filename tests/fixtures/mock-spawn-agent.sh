#!/bin/bash
#mock spawn-agent-v2.sh for integration testing
#echoes back a result or fails based on agent name

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

#simulate failure for specific agent names
if [[ "$AGENT" == *"fail"* ]] || [[ "$AGENT" == *"nonexistent"* ]]; then
    echo "ERROR: Agent '$AGENT' not found or execution failed" >&2
    exit 1
fi

#extract component number from prompt
COMPONENT=$(echo "$PROMPT" | grep -oP 'Component \K\d+' | head -1)

#simulate work output
echo "Mock agent '$AGENT' completed Component $COMPONENT successfully. Model: $MODEL. Timestamp: $(date -Iseconds)"
