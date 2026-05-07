"""Shared on_failure_callback for all DAGs in this project.

Fires only after retries are exhausted (Airflow calls the callback
only on terminal failure of the task instance — not on each retry
attempt — so this is automatic).

Two-stage notification:
  1. Immediate text alert: which DAG/task failed, when, log URL.
     Uses the Signal client directly. Fast (< 5s).
  2. Optional Claude-driven diagnosis: fires a fire-and-forget
     subprocess that reads the log + code, asks Claude Code to
     analyze, and posts the analysis as a follow-up Signal message.
     Off-process so airflow worker isn't blocked.

The diagnosis stage is opt-in via the SIGNAL_DIAGNOSIS_ENABLED env
var — Day 1 of the rollout we're enabling only stage 1.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# This file lives at airflow/dags/_alerts.py inside the container,
# but the source mounts give us /opt/airflow/src as the import root.
sys.path.insert(0, "/opt/airflow/src")

from stock_screening.signal_alerts.client import send as signal_send  # noqa: E402

logger = logging.getLogger(__name__)


def _format_alert(context: dict) -> str:
    ti = context.get("task_instance")
    dag_id = context["dag"].dag_id if "dag" in context else "?"
    task_id = ti.task_id if ti else "?"
    run_id = ti.run_id if ti else "?"
    log_url = ti.log_url if ti and hasattr(ti, "log_url") else "?"
    when = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    exc = context.get("exception")
    exc_short = str(exc).splitlines()[0][:200] if exc else "(no exception in context)"
    return (
        f"⚠️ Airflow failure\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Run: {run_id}\n"
        f"Time: {when}\n"
        f"Error: {exc_short}\n"
        f"Log: {log_url}"
    )


def _spawn_diagnosis(context: dict) -> None:
    """Fire-and-forget Claude-driven diagnosis. The actual heavy
    lifting lives in scripts/diagnose_and_notify.sh on the host, run
    via a docker exec hop or directly if invoked outside container."""
    if os.environ.get("SIGNAL_DIAGNOSIS_ENABLED", "").lower() not in ("1", "true", "yes"):
        return
    script = Path("/opt/airflow/scripts/diagnose_and_notify.sh")
    if not script.is_file():
        logger.warning("diagnosis script %s not found; skipping", script)
        return
    ti = context.get("task_instance")
    env = {
        **os.environ,
        "DIAG_DAG_ID": context["dag"].dag_id,
        "DIAG_TASK_ID": ti.task_id if ti else "",
        "DIAG_RUN_ID": ti.run_id if ti else "",
        "DIAG_LOG_URL": ti.log_url if ti and hasattr(ti, "log_url") else "",
    }
    try:
        # Popen + close stdout/stderr → doesn't keep worker tied to child
        subprocess.Popen(
            [str(script)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        logger.info("diagnosis script spawned for %s.%s", env["DIAG_DAG_ID"], env["DIAG_TASK_ID"])
    except Exception as e:
        # Never let the alert path fail the task callback
        logger.exception("failed to spawn diagnosis script: %s", e)


def alert_on_failure(context: dict) -> None:
    """Airflow on_failure_callback. Best-effort — never raises."""
    try:
        signal_send(_format_alert(context))
    except Exception as e:
        # Log but don't propagate; otherwise we mask the real failure
        # behind a callback failure.
        logger.exception("signal send failed: %s", e)
    try:
        _spawn_diagnosis(context)
    except Exception as e:
        logger.exception("diagnosis spawn failed: %s", e)
