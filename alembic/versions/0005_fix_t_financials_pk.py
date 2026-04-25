"""relax t_financials.periodStart NOT NULL and switch PK to periodEnd

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-25

XBRL `CurrentYearInstant` facts (balance sheet items — exactly the
facts the screen needs) have a `period_end` but no `period_start`.
The original prototype had no PK so this never came up; the refactor's
NOT NULL + PK on `periodStart` blocked all balance sheet ingestion.

Fix: make `periodStart` nullable; switch PK to (docID, itemName,
periodEnd, categoryID) — periodEnd is always set on relevant facts.
"""

import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("t_financials_pkey", "t_financials", type_="primary")
    op.alter_column(
        "t_financials", "periodStart", existing_type=sa.Date(), nullable=True
    )
    op.alter_column(
        "t_financials", "periodEnd", existing_type=sa.Date(), nullable=False
    )
    op.create_primary_key(
        "t_financials_pkey",
        "t_financials",
        ["docID", "itemName", "periodEnd", "categoryID"],
    )


def downgrade() -> None:
    op.drop_constraint("t_financials_pkey", "t_financials", type_="primary")
    op.alter_column(
        "t_financials", "periodEnd", existing_type=sa.Date(), nullable=True
    )
    op.alter_column(
        "t_financials", "periodStart", existing_type=sa.Date(), nullable=False
    )
    op.create_primary_key(
        "t_financials_pkey",
        "t_financials",
        ["docID", "itemName", "periodStart", "categoryID"],
    )
