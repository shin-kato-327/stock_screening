"""create bot_readonly postgres role for the Telegram Q&A bot

Revision ID: 0012
Revises: 0011
Create Date: 2026-05-09

Read-only role used by the Telegram Q&A bot to query screening data.
Three layers of write protection:
  1. No INSERT/UPDATE/DELETE/DDL grants.
  2. default_transaction_read_only = on at role level.
  3. statement_timeout = 15s to cap runaway SELECTs.

Granted SELECT on the screen-relevant tables. t_sim_* deliberately
excluded — those are scratch tables, not user-facing data.

Password is read from POSTGRES_BOT_READONLY_PASSWORD env at upgrade
time. CREATE ROLE doesn't accept libpq bind params for the password
literal, so we validate the env value and f-string it. Idempotent
upgrade (DO $$ ... IF NOT EXISTS ...) and clean downgrade
(REASSIGN OWNED + DROP OWNED + DROP ROLE).

Must run as a postgres superuser; aborts otherwise.
"""

from __future__ import annotations

import os
import re

from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None

ROLE = "bot_readonly"
GRANTED_TABLES = (
    "t_doc_list",
    "t_edinet_code_mappings",
    "t_financials",
    "t_financials_annual",
    "t_daily_stock_perf",
    "t_screen_results",
)
PW_PATTERN = re.compile(r"^[A-Za-z0-9_./=+\-]{20,}$")


def _superuser_guard() -> None:
    bind = op.get_bind()
    is_super = bind.exec_driver_sql("SELECT current_setting('is_superuser')").scalar()
    if str(is_super).lower() != "on":
        raise RuntimeError(
            f"migration 0012 must run as a postgres superuser; got is_superuser={is_super!r}"
        )


def _read_password() -> str:
    pw = os.environ.get("POSTGRES_BOT_READONLY_PASSWORD")
    if not pw:
        raise RuntimeError(
            "POSTGRES_BOT_READONLY_PASSWORD env var must be set when running migration 0012"
        )
    if not PW_PATTERN.match(pw):
        raise RuntimeError(
            "POSTGRES_BOT_READONLY_PASSWORD must match [A-Za-z0-9_./=+-]{20,} "
            "(at least 20 chars, no shell-special chars). "
            "Generate one with: python -c \"import secrets; print(secrets.token_urlsafe(24))\""
        )
    return pw


def upgrade() -> None:
    _superuser_guard()
    pw = _read_password()

    # Create role idempotently. Use DO $$ ... $$ so re-running the
    # migration on a system that already has the role is a no-op.
    # NB: format() with %L double-quotes the literal correctly.
    op.execute(
        f"""
        DO $$ BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='{ROLE}') THEN
            EXECUTE format('CREATE ROLE {ROLE} LOGIN PASSWORD %L', '{pw}');
          ELSE
            EXECUTE format('ALTER ROLE {ROLE} WITH LOGIN PASSWORD %L', '{pw}');
          END IF;
        END $$;
        """
    )

    # Lock down schema-level privileges, then grant only what we need.
    op.execute(f"REVOKE ALL ON SCHEMA public FROM {ROLE}")
    op.execute(f"GRANT USAGE ON SCHEMA public TO {ROLE}")
    op.execute(f"REVOKE TEMP ON DATABASE financial_data FROM {ROLE}")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {ROLE}")

    # Table grants — strictly SELECT.
    op.execute(
        f"GRANT SELECT ON {', '.join(GRANTED_TABLES)} TO {ROLE}"
    )

    # Role-level GUCs — belt + suspenders.
    op.execute(f"ALTER ROLE {ROLE} SET default_transaction_read_only = on")
    op.execute(f"ALTER ROLE {ROLE} SET statement_timeout = '15s'")
    op.execute(f"ALTER ROLE {ROLE} SET idle_in_transaction_session_timeout = '30s'")
    op.execute(f"ALTER ROLE {ROLE} SET lock_timeout = '5s'")


def downgrade() -> None:
    _superuser_guard()
    # Order matters: must release ownership and revoke grants before
    # DROP ROLE will succeed.
    op.execute(f"REASSIGN OWNED BY {ROLE} TO root")
    op.execute(f"DROP OWNED BY {ROLE}")
    op.execute(f"DROP ROLE IF EXISTS {ROLE}")
