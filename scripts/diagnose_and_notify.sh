#!/usr/bin/env bash
# Claude-driven diagnosis of an Airflow task failure.
# Invoked by airflow/dags/_alerts.py on terminal task failure (after
# retries exhausted). Read-only — Claude is restricted to Read + a
# narrow shell allowlist; cannot Edit, Write, push, or run the DB.
#
# Inputs (env): DIAG_DAG_ID, DIAG_TASK_ID, DIAG_RUN_ID, DIAG_LOG_URL
# Outputs: a Signal message with root cause + suggested fix diff.
#
# Cost guard: --max-turns 30, ~5 diagnoses/day rate limit (file-based).

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$HOME/workspace/stock_screening}"
DIAG_DIR="${DIAG_DIR:-$HOME/diagnostics}"
RATE_LIMIT_FILE="${DIAG_DIR}/rate_limit.txt"
MAX_PER_DAY="${MAX_PER_DAY:-5}"

mkdir -p "$DIAG_DIR"

# ---- rate limit (best-effort, racy but fine for our scale) ----
today="$(date -u +%Y-%m-%d)"
count=0
if [ -f "$RATE_LIMIT_FILE" ]; then
    count=$(grep -c "^$today " "$RATE_LIMIT_FILE" || echo 0)
fi
if [ "$count" -ge "$MAX_PER_DAY" ]; then
    echo "rate limit hit ($count/$MAX_PER_DAY for $today); skipping diagnosis" >&2
    # Send the cap notice so the user knows we deliberately skipped.
    PYTHONPATH="$REPO_ROOT/src" python3 -c "
from stock_screening.signal_alerts.client import send
send(f'⏸  Diagnosis skipped: $MAX_PER_DAY/day cap reached. ${DIAG_DAG_ID}.${DIAG_TASK_ID} failed.')
" || true
    exit 0
fi
echo "$today $(date -u +%H:%M:%S) ${DIAG_DAG_ID}.${DIAG_TASK_ID}" >> "$RATE_LIMIT_FILE"

# ---- run id / log path discovery ----
TS="$(date -u +%Y%m%dT%H%M%SZ)"
SESSION_DIR="$DIAG_DIR/${TS}_${DIAG_DAG_ID:-unknown}_${DIAG_TASK_ID:-unknown}"
mkdir -p "$SESSION_DIR"
ANALYSIS_FILE="$SESSION_DIR/analysis.md"
LOG_FILE="$SESSION_DIR/claude.log"

# Find the actual log file path inside the airflow container's logs dir.
# We assume the airflow stack is in ~/workspace/stock_screening/airflow.
AIRFLOW_LOGS="$REPO_ROOT/airflow/logs"
LATEST_TASK_LOG="$(find "$AIRFLOW_LOGS" -name "*.log" \
    -path "*dag_id=${DIAG_DAG_ID}*" \
    -path "*run_id=${DIAG_RUN_ID//\//__}*" \
    -path "*task_id=${DIAG_TASK_ID}*" \
    2>/dev/null | sort | tail -1 || true)"

# Build the prompt for Claude. Read-only — analysis only.
PROMPT="$(cat <<EOF
You are diagnosing an Airflow task failure on a small data-engineering project.
You are running in READ-ONLY mode: you can read files and run a narrow set
of shell commands (git log, grep, find, head/tail, wc). You may NOT edit
files, commit, push, or modify the database.

Project root: $REPO_ROOT
DAG: $DIAG_DAG_ID
Task: $DIAG_TASK_ID
Run: $DIAG_RUN_ID
Failure log: $LATEST_TASK_LOG

Steps:
1. Read the failure log (tail it if very long; the actionable error is usually near the end).
2. Read the DAG file at airflow/dags/$DIAG_DAG_ID.py and any library code under src/stock_screening/ that the failing task touches.
3. Check git log for recent commits to those files: did a recent change introduce this?
4. Identify the root cause. Be specific — name the line and the exact error mode.
5. Propose a fix as a unified diff (do NOT apply it).
6. Rate confidence: high / medium / low.
7. Recommend next steps (PR? backfill? config change?).

Respond in this exact Markdown structure (no preamble):

## Root cause
<1-3 sentences>

## Proposed fix
\`\`\`diff
<unified diff against the file(s) that need changing>
\`\`\`

## Confidence
<high|medium|low> — <one-sentence justification>

## Next steps
1. <step>
2. <step>

Keep total response under 1500 characters of prose plus the diff.
EOF
)"

# ---- invoke Claude Code in headless mode with restricted tools ----
# Falls back gracefully if `claude` is not installed.
if ! command -v claude > /dev/null 2>&1; then
    echo "claude CLI not on PATH; cannot diagnose" >&2
    PYTHONPATH="$REPO_ROOT/src" python3 -c "
from stock_screening.signal_alerts.client import send
send(f'⚠️  ${DIAG_DAG_ID}.${DIAG_TASK_ID} failed; Claude diagnosis unavailable (CLI missing).')
" || true
    exit 1
fi

cd "$REPO_ROOT"
claude \
    --print \
    --max-turns 30 \
    --output-format text \
    --allowedTools "Read,Bash(git:*),Bash(grep:*),Bash(find:*),Bash(head:*),Bash(tail:*),Bash(wc:*),Bash(ls:*),Bash(cat:*)" \
    --disallowedTools "Edit,Write,NotebookEdit,Bash(rm:*),Bash(mv:*),Bash(docker:*),Bash(psql:*),Bash(curl:*),Bash(ssh:*),Bash(scp:*)" \
    "$PROMPT" > "$ANALYSIS_FILE" 2> "$LOG_FILE" || {
    echo "claude invocation failed; see $LOG_FILE" >&2
    PYTHONPATH="$REPO_ROOT/src" python3 -c "
from stock_screening.signal_alerts.client import send
send(f'⚠️  ${DIAG_DAG_ID}.${DIAG_TASK_ID} failed; Claude diagnosis errored. See $SESSION_DIR.')
" || true
    exit 1
}

# ---- post the analysis to Signal ----
PYTHONPATH="$REPO_ROOT/src" python3 - "$ANALYSIS_FILE" "$DIAG_DAG_ID" "$DIAG_TASK_ID" "$DIAG_LOG_URL" <<'PY'
import sys
from pathlib import Path
from stock_screening.signal_alerts.client import send

analysis_path = Path(sys.argv[1])
dag_id, task_id, log_url = sys.argv[2], sys.argv[3], sys.argv[4]
analysis = analysis_path.read_text()

header = f"🔍 Diagnosis: {dag_id}.{task_id}\nLog: {log_url}\n"
short = header + analysis
send(short, attachments=[analysis_path])
PY

echo "diagnosis complete: $SESSION_DIR" >&2
