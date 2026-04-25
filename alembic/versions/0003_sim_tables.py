"""sim tables — screen results, positions, trades, NAV

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-24

Tables for the daily value-screen output and the paper-trading simulation.

- t_screen_results: one row per (run_date, secCode) with the computed
  net-cash ratio. FK to t_financials_annual on (secCode, period_end)
  so the underlying financial inputs are reconstructable without
  duplicating them here.

- t_sim_positions: end-of-day snapshot. PK (as_of_date, secCode).

- t_sim_trades: surrogate bigserial PK so partial fills / multi-trades-
  per-day are not blocked. Unique constraint on the natural key
  prevents accidental duplicates from re-runs.

- t_sim_portfolio_nav: one row per day. Includes benchmark NAV (1306.T
  buy-and-hold) for comparison.
"""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "t_screen_results",
        sa.Column("run_date", sa.Date(), nullable=False),
        sa.Column("secCode", sa.String(50), nullable=False),
        sa.Column("ratio", sa.Numeric(12, 6)),
        sa.Column("qualifies", sa.Boolean(), nullable=False),
        sa.Column("source_period_end", sa.Date(), nullable=False),
        sa.Column("market_cap", sa.BigInteger()),
        sa.Column(
            "computed_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("run_date", "secCode"),
        sa.ForeignKeyConstraint(
            ["secCode", "source_period_end"],
            ["t_financials_annual.secCode", "t_financials_annual.period_end"],
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "ix_t_screen_results_run_date_qualifies",
        "t_screen_results",
        ["run_date", "qualifies"],
    )

    op.create_table(
        "t_sim_positions",
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("secCode", sa.String(50), nullable=False),
        sa.Column("shares", sa.Integer(), nullable=False),
        sa.Column("avg_cost", sa.Numeric(20, 4), nullable=False),
        sa.Column("last_price", sa.Numeric(20, 4), nullable=False),
        sa.Column("market_value", sa.Numeric(20, 4), nullable=False),
        sa.PrimaryKeyConstraint("as_of_date", "secCode"),
    )

    op.create_table(
        "t_sim_trades",
        sa.Column("trade_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("secCode", sa.String(50), nullable=False),
        sa.Column("side", sa.CHAR(1), nullable=False),
        sa.Column("shares", sa.Integer(), nullable=False),
        sa.Column("price", sa.Numeric(20, 4), nullable=False),
        sa.Column("commission", sa.Numeric(20, 4), nullable=False),
        sa.Column("reason", sa.Text()),
        sa.CheckConstraint("side IN ('B', 'S')", name="ck_t_sim_trades_side"),
        sa.UniqueConstraint(
            "trade_date",
            "secCode",
            "side",
            "shares",
            "price",
            name="uq_t_sim_trades_natural",
        ),
    )
    op.create_index("ix_t_sim_trades_trade_date", "t_sim_trades", ["trade_date"])

    op.create_table(
        "t_sim_portfolio_nav",
        sa.Column("as_of_date", sa.Date(), primary_key=True),
        sa.Column("cash", sa.Numeric(20, 4), nullable=False),
        sa.Column("positions_value", sa.Numeric(20, 4), nullable=False),
        sa.Column("total_nav", sa.Numeric(20, 4), nullable=False),
        sa.Column("benchmark_nav", sa.Numeric(20, 4)),
        sa.Column("n_positions", sa.Integer(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("t_sim_portfolio_nav")
    op.drop_table("t_sim_trades")
    op.drop_table("t_sim_positions")
    op.drop_table("t_screen_results")
