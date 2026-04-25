"""add issued_shares to financials annual mart

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-24

Required so the daily price DAG can compute marketCap = close *
issued_shares without depending on yfinance (which the prototype
hit and which is unreliable for JP tickers).

issued_shares is captured from XBRL by the parser (concept lookup in
financials/concepts.py) and pivoted into the mart by build_annual_mart.
"""

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "t_financials_annual",
        sa.Column("issued_shares", sa.Numeric(20, 0)),
    )


def downgrade() -> None:
    op.drop_column("t_financials_annual", "issued_shares")
