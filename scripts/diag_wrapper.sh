#!/usr/bin/env bash
# Forced-command wrapper. Allowlists DIAG_* env-var assignments from
# $SSH_ORIGINAL_COMMAND, sources the project .env so the host-side
# diagnose script has TELEGRAM_BOT_TOKEN / CHAT_ID, then exec's the
# real diagnose script through a login shell (so claude is on PATH).
set -euo pipefail
for tok in $SSH_ORIGINAL_COMMAND; do
    if [[ "$tok" =~ ^DIAG_[A-Z_]+=.+$ ]]; then
        export "$tok"
    fi
done
# Load TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, etc. from the project .env.
if [ -f /home/shinkato/workspace/stock_screening/.env ]; then
    set -a
    # shellcheck disable=SC1091
    . /home/shinkato/workspace/stock_screening/.env
    set +a
fi
exec bash -lc "/home/shinkato/workspace/stock_screening/scripts/diagnose_and_notify.sh"
