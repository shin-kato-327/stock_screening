"""Shared on_failure_callback for all DAGs in this project.

Two-stage notification:
  1. Immediate text alert via Telegram: which DAG/task failed, when,
     log URL. Uses the Telegram client directly. Fast (< 5s).
  2. Optional Claude-driven diagnosis: fires a fire-and-forget
     subprocess that reads the log + code, asks Claude Code to
     analyze, and posts the analysis as a follow-up Telegram message.
     Off-process so airflow worker isn't blocked.

The diagnosis stage is opt-in via the TELEGRAM_DIAGNOSIS_ENABLED env
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

from airflow.models import Variable  # noqa: E402

from stock_screening.telegram_alerts.client import send as telegram_send  # noqa: E402

logger = logging.getLogger(__name__)


def _hydrate_env_from_variables() -> None:
    """Mirror Airflow Variables into os.environ so the pure-Python
    Telegram client (which reads from os.environ) works inside an
    Airflow task context. Idempotent — only sets if not already set."""
    for name in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        if name not in os.environ:
            try:
                value = Variable.get(name, default_var=None)
            except Exception:
                value = None
            if value:
                os.environ[name] = value


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
    lifting lives in scripts/diagnose_and_notify.sh on the host."""
    if os.environ.get("TELEGRAM_DIAGNOSIS_ENABLED", "").lower() not in ("1", "true", "yes"):
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
        subprocess.Popen(
            [str(script)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        logger.info("diagnosis script spawned for %s.%s", env["DIAG_DAG_ID"], env["DIAG_TASK_ID"])
    except Exception as e:
        logger.exception("failed to spawn diagnosis script: %s", e)


def alert_on_failure(context: dict) -> None:
    """Airflow on_failure_callback. Best-effort — never raises."""
    _hydrate_env_from_variables()
    try:
        telegram_send(_format_alert(context))
    except Exception as e:
        logger.exception("telegram send failed: %s", e)
    try:
        _spawn_diagnosis(context)
    except Exception as e:
        logger.exception("diagnosis spawn failed: %s", e)
