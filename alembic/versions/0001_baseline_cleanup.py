"""baseline cleanup schema

Revision ID: 0001
Revises:
Create Date: 2026-04-24

Canonical schema for financial_data on Postgres 16+.

Replaces the legacy prototype schema (kept as rollback in ./financial_data,
pg13). Differences vs. legacy:
- PKs everywhere; FK from t_financials.docID → t_doc_list.docID.
- Date-like columns are DATE / TIMESTAMPTZ instead of String(50).
- amount is NUMERIC(20, 4) instead of BIGINT (XBRL ratios have decimals).
- previousClose renamed to close (the column is the day's close, not the prior day's).
- t_short_list dropped (was duplicating t_daily_stock_perf).
- New columns:
    t_financials.currency_code   (CHAR(3) NOT NULL DEFAULT 'JPY')
    t_financials.concept_id      (XBRL concept, e.g. jpcrp_cor:CurrentAssets)
    t_doc_list.is_latest_for_period (set at ingest; resolves the supersedes-on-amendment problem)

Data is repopulated by scripts/bootstrap_history.sh via EDINET + JQUANTS APIs.
"""

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "t_doc_list",
        sa.Column("docID", sa.String(50), primary_key=True),
        sa.Column("edinetCode", sa.String(50)),
        sa.Column("secCode", sa.String(50)),
        sa.Column("JCN", sa.String(50)),
        sa.Column("filerName", sa.String(100)),
        sa.Column("fundCode", sa.String(50)),
        sa.Column("ordinanceCode", sa.String(50)),
        sa.Column("formCode", sa.String(50)),
        sa.Column("docTypeCode", sa.String(50)),
        sa.Column("periodStart", sa.Date()),
        sa.Column("periodEnd", sa.Date()),
        sa.Column("submitDateTime", sa.TIMESTAMP(timezone=True)),
        sa.Column("docDescription", sa.String(255)),
        sa.Column("issuerEdinetCode", sa.String(50)),
        sa.Column("subjectEdinetCode", sa.String(50)),
        sa.Column("currentReportReason", sa.String(255)),
        sa.Column("parentDocID", sa.String(50)),
        sa.Column("opeDateTime", sa.TIMESTAMP(timezone=True)),
        sa.Column("xbrlFlag", sa.Integer()),
        sa.Column("pdfFlag", sa.Integer()),
        sa.Column("csvFlag", sa.Integer()),
        sa.Column(
            "is_latest_for_period",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.create_index("ix_t_doc_list_edinetCode", "t_doc_list", ["edinetCode"])
    op.create_index("ix_t_doc_list_secCode", "t_doc_list", ["secCode"])
    op.create_index("ix_t_doc_list_submitDateTime", "t_doc_list", ["submitDateTime"])

    op.create_table(
        "t_edinet_code_mappings",
        sa.Column("edinetCode", sa.String(50), primary_key=True),
        sa.Column("issuerCategory", sa.String(50)),
        sa.Column("jojoKubun", sa.String(50)),
        sa.Column("consolidatedKubun", sa.String(50)),
        sa.Column("fundedAmt", sa.BigInteger()),
        sa.Column("endOfFiscalYearDt", sa.Date()),
        sa.Column("issuerNameJP", sa.String(100)),
        sa.Column("issuerNameEN", sa.String(200)),
        sa.Column("addressJP", sa.String(200)),
        sa.Column("industryCat", sa.String(100)),
        sa.Column("securityCode", sa.String(50)),
        sa.Column("houjinNumber", sa.String(50)),
    )
    op.create_index(
        "ix_t_edinet_code_mappings_securityCode",
        "t_edinet_code_mappings",
        ["securityCode"],
    )

    op.create_table(
        "t_financials",
        sa.Column("docID", sa.String(50), nullable=False),
        sa.Column("itemName", sa.String(255), nullable=False),
        sa.Column("amount", sa.Numeric(20, 4)),
        sa.Column("periodStart", sa.Date(), nullable=False),
        sa.Column("periodEnd", sa.Date()),
        sa.Column("categoryID", sa.String(50), nullable=False),
        sa.Column("concept_id", sa.String(255)),
        sa.Column(
            "currency_code",
            sa.CHAR(3),
            nullable=False,
            server_default="JPY",
        ),
        sa.PrimaryKeyConstraint("docID", "itemName", "periodStart", "categoryID"),
        sa.ForeignKeyConstraint(
            ["docID"], ["t_doc_list.docID"], ondelete="CASCADE"
        ),
    )
    op.create_index("ix_t_financials_itemName", "t_financials", ["itemName"])
    op.create_index("ix_t_financials_concept_id", "t_financials", ["concept_id"])

    op.create_table(
        "t_daily_stock_perf",
        sa.Column("Date", sa.Date(), nullable=False),
        sa.Column("ShokenCode", sa.String(50), nullable=False),
        sa.Column("CompanyName", sa.String(100)),
        sa.Column("CompanyNameEnglish", sa.String(200)),
        sa.Column("Sector17Code", sa.String(10)),
        sa.Column("Sector17CodeName", sa.String(200)),
        sa.Column("Sector33Code", sa.String(10)),
        sa.Column("Sector33CodeName", sa.String(200)),
        sa.Column("ScaleCategory", sa.String(50)),
        sa.Column("MarketCode", sa.String(10)),
        sa.Column("MarketCodeName", sa.String(50)),
        sa.Column("MarketCodeCleansed", sa.String(50)),
        sa.Column("close", sa.Integer()),
        sa.Column("trailingPE", sa.Numeric(18, 10)),
        sa.Column("volume", sa.BigInteger()),
        sa.Column("marketCap", sa.BigInteger()),
        sa.Column("fiftyTwoWeekLow", sa.Integer()),
        sa.Column("fiftyTwoWeekHigh", sa.Integer()),
        sa.Column("revenuePerShare", sa.Numeric(18, 10)),
        sa.PrimaryKeyConstraint("Date", "ShokenCode"),
    )
    # Composite index supports the common "latest N days for one stock" query.
    op.execute(
        'CREATE INDEX "ix_t_daily_stock_perf_ShokenCode_Date" '
        'ON t_daily_stock_perf ("ShokenCode", "Date" DESC)'
    )


def downgrade() -> None:
    op.drop_table("t_daily_stock_perf")
    op.drop_table("t_financials")
    op.drop_table("t_edinet_code_mappings")
    op.drop_table("t_doc_list")
