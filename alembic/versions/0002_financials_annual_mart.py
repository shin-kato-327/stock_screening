"""financials annual mart

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-24

Derived typed table that the screen queries instead of raw EAV
(t_financials). One row per (secCode, period_end). Components stored
explicitly so users can audit which pieces drove interest_bearing_debt
(some companies report it as a single line, others split four ways:
短期借入金 + 長期借入金 + 社債 + リース債務).

interest_bearing_debt is a GENERATED STORED column — Postgres computes
it on insert/update from the four component columns, so the typed
total is always consistent with the parts.

Built by src/stock_screening/financials/build_annual_mart.py from raw
EAV rows, called as a task in edinet_xbrl_ingest_dag after parsing.
"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "t_financials_annual",
        sa.Column("secCode", sa.String(50), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("fiscal_year_end_dt", sa.Date()),
        sa.Column("source_doc_id", sa.String(50), nullable=False),
        # Balance sheet components
        sa.Column("current_assets", sa.Numeric(20, 4)),
        sa.Column("short_term_borrowings", sa.Numeric(20, 4)),
        sa.Column("long_term_borrowings", sa.Numeric(20, 4)),
        sa.Column("bonds", sa.Numeric(20, 4)),
        sa.Column("lease_obligations", sa.Numeric(20, 4)),
        sa.Column("investment_securities", sa.Numeric(20, 4)),
        sa.Column("total_liabilities", sa.Numeric(20, 4)),
        sa.Column("total_assets", sa.Numeric(20, 4)),
        # Audit
        sa.Column(
            "ingested_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("secCode", "period_end"),
        sa.ForeignKeyConstraint(
            ["source_doc_id"], ["t_doc_list.docID"], ondelete="RESTRICT"
        ),
    )

    # GENERATED STORED column: total interest-bearing debt = sum of components.
    # Alembic doesn't have first-class support for GENERATED, so use raw SQL.
    op.execute(
        """
        ALTER TABLE t_financials_annual
        ADD COLUMN interest_bearing_debt NUMERIC(20, 4)
        GENERATED ALWAYS AS (
            COALESCE(short_term_borrowings, 0)
          + COALESCE(long_term_borrowings, 0)
          + COALESCE(bonds, 0)
          + COALESCE(lease_obligations, 0)
        ) STORED
        """
    )

    op.create_index(
        "ix_t_financials_annual_period_end",
        "t_financials_annual",
        ["period_end"],
    )


def downgrade() -> None:
    op.drop_table("t_financials_annual")
