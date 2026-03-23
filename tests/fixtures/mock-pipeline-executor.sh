#!/bin/bash
#mock pipeline executor for integration testing
#watches ready/ dir for task_pulsar_* files,
#simulates execution by moving them to completed/ with a result

READY_DIR="$1"
COMPLETED_DIR="$2"

if [[ -z "$READY_DIR" || -z "$COMPLETED_DIR" ]]; then
    echo "Usage: mock-pipeline-executor.sh <ready_dir> <completed_dir>" >&2
    exit 1
fi

mkdir -p "$COMPLETED_DIR"

#poll every 0.5s for new task files
while true; do
    for file in "$READY_DIR"/task_pulsar_*.md; do
        [[ -f "$file" ]] || continue

        filename=$(basename "$file")
        #read the agent name from the task file
        agent=$(grep -oP '\*\*Target Agent:\*\*\s*\K\S+' "$file" 2>/dev/null || echo "unknown")

        #simulate failure for specific agent names
        if [[ "$agent" == *"fail"* ]] || [[ "$agent" == *"nonexistent"* ]]; then
            #write a failed result and move to completed
            {
                cat "$file"
                echo ""
                echo "**Result:** ERROR: Agent '$agent' not found or execution failed"
                echo "**Execution Status:** FAILED"
            } > "$COMPLETED_DIR/$filename"
        else
            #extract component number from filename
            component=$(echo "$filename" | grep -oP '_(\d+)\.md$' | grep -oP '\d+')
            #write success result and move to completed
            {
                cat "$file"
                echo ""
                echo "**Result:** Mock pipeline executor completed Component $component successfully. Agent: $agent. Timestamp: $(date -Iseconds)"
                echo "**Execution Status:** COMPLETED"
            } > "$COMPLETED_DIR/$filename"
        fi

        #remove from ready (simulates executor picking it up)
        rm -f "$file"
    done

    sleep 0.5
done
