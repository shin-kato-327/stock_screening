"""add operating_income to financials annual mart

Revision ID: 0009
Revises: 0008
Create Date: 2026-04-26

Net-cash screen alone produces value traps — companies with structural
net cash but stagnant or declining operations that the market correctly
prices low (e.g. 双葉電子工業, コロナ in our 2022 cohort). Adding an
operating-income filter lets the screen require "solid profitability"
alongside net cash.

Operating income is a CurrentYearDuration fact (income-statement flow,
not balance-sheet snapshot). build_annual_mart.py is updated to query
both Instant and Duration facts — the Duration filings have the same
period_end as the Instant balance-sheet snapshot, so they collapse onto
the same mart row.
"""

import sqlalchemy as sa
from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "t_financials_annual",
        sa.Column("operating_income", sa.Numeric(20, 4)),
    )


def downgrade() -> None:
    op.drop_column("t_financials_annual", "operating_income")
