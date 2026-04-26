"""add net_sales to financials annual mart

Revision ID: 0010
Revises: 0009
Create Date: 2026-04-26

Net sales (売上高) lets the screen distinguish real turnarounds (op
income recovering on RISING revenue) from one-off cost cuts (op income
recovering with FLAT revenue — usually fragile).

Same data path as operating_income: it's a CurrentYearDuration fact
that's already in t_financials. New mart column lets the screen
self-join to compute year-over-year sales growth alongside op income
growth.
"""

import sqlalchemy as sa
from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "t_financials_annual",
        sa.Column("net_sales", sa.Numeric(20, 4)),
    )


def downgrade() -> None:
    op.drop_column("t_financials_annual", "net_sales")
