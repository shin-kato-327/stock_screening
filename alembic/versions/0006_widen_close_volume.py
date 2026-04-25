"""widen close to NUMERIC and volume to BIGINT

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-25

JQuants daily_quotes returns float closes (e.g. 396.9 for ETFs) and
multi-billion volumes for high-turnover stocks. The original schema
had close=INTEGER and volume=INTEGER (the latter was already widened
to BIGINT in 0001 but the code review caught only volume; close kept
INTEGER). This caps overflow risk for both.
"""

import sqlalchemy as sa
from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "t_daily_stock_perf",
        "close",
        existing_type=sa.Integer(),
        type_=sa.Numeric(18, 4),
        postgresql_using="close::numeric(18,4)",
    )
    op.alter_column(
        "t_daily_stock_perf",
        "volume",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        postgresql_using="volume::bigint",
    )


def downgrade() -> None:
    op.alter_column(
        "t_daily_stock_perf",
        "volume",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        postgresql_using="volume::integer",
    )
    op.alter_column(
        "t_daily_stock_perf",
        "close",
        existing_type=sa.Numeric(18, 4),
        type_=sa.Integer(),
        postgresql_using="close::integer",
    )
