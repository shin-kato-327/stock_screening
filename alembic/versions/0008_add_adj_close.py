"""add split-adjusted close + adjustment factor to t_daily_stock_perf

Revision ID: 0008
Revises: 0007
Create Date: 2026-04-25

The original price ingest stored only the unadjusted close (`C` from
JQuants v2). Backtests that span a stock split show fake price
collapses on the split day — caught when 1306 (TOPIX ETF) split ~10:1
between Dec 2025 and Mar 2026 and our benchmark NAV "dropped" 86%
overnight.

JQuants v2 daily_quotes already returns `AdjC` (split-adjusted close)
and `AdjFactor` per row; we just need columns to put them in. Backfill
is done by re-running `backfill_window.py --phase=prices`, which now
populates these alongside the existing `close` and `marketCap`.
"""

import sqlalchemy as sa
from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "t_daily_stock_perf",
        sa.Column("adj_close", sa.Numeric(18, 4)),
    )
    op.add_column(
        "t_daily_stock_perf",
        sa.Column("adj_factor", sa.Numeric(18, 8)),
    )


def downgrade() -> None:
    op.drop_column("t_daily_stock_perf", "adj_factor")
    op.drop_column("t_daily_stock_perf", "adj_close")
