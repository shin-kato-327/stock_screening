"""add net_income to financials annual mart

Revision ID: 0011
Revises: 0010
Create Date: 2026-04-26

PER (株価収益率) = market_cap / net_income. Adding net_income to the
mart lets the screen filter for low-PER + high-net-cash candidates,
matching the Shikiho 28-stock screen approach.

Source XBRL fact lives in CurrentYearDuration just like operating_income
and net_sales. Already extracted by the parser; just needs a column to
pivot into.
"""

import sqlalchemy as sa
from alembic import op

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "t_financials_annual",
        sa.Column("net_income", sa.Numeric(20, 4)),
    )


def downgrade() -> None:
    op.drop_column("t_financials_annual", "net_income")
