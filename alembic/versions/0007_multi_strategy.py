"""multi-strategy: scope sim tables by strategy_name

Revision ID: 0007
Revises: 0006
Create Date: 2026-04-25

Adds strategy_name to t_sim_positions, t_sim_trades, and
t_sim_portfolio_nav so multiple strategies can share the same screen
output but maintain independent portfolios.

Existing rows are backfilled as 'netcash_top20_equal' (the baseline
that the original sim DAG was running). Composite keys are rebuilt
with strategy_name as the leading column.
"""

import sqlalchemy as sa
from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None

DEFAULT_STRATEGY = "netcash_top20_equal"


def upgrade() -> None:
    # 1) Add column nullable, backfill, then make NOT NULL.
    for table in ("t_sim_positions", "t_sim_trades", "t_sim_portfolio_nav"):
        op.add_column(table, sa.Column("strategy_name", sa.String(50)))
        op.execute(
            f"UPDATE {table} SET strategy_name = '{DEFAULT_STRATEGY}' WHERE strategy_name IS NULL"
        )
        op.alter_column(table, "strategy_name", existing_type=sa.String(50), nullable=False)

    # 2) Rebuild constraints with strategy_name as leading column.
    op.drop_constraint("t_sim_positions_pkey", "t_sim_positions", type_="primary")
    op.create_primary_key(
        "t_sim_positions_pkey",
        "t_sim_positions",
        ["strategy_name", "as_of_date", "secCode"],
    )

    op.drop_constraint("uq_t_sim_trades_natural", "t_sim_trades", type_="unique")
    op.create_unique_constraint(
        "uq_t_sim_trades_natural",
        "t_sim_trades",
        ["strategy_name", "trade_date", "secCode", "side", "shares", "price"],
    )
    # trade_id surrogate PK already exists, no change needed.

    op.drop_constraint("t_sim_portfolio_nav_pkey", "t_sim_portfolio_nav", type_="primary")
    op.create_primary_key(
        "t_sim_portfolio_nav_pkey",
        "t_sim_portfolio_nav",
        ["strategy_name", "as_of_date"],
    )


def downgrade() -> None:
    op.drop_constraint("t_sim_portfolio_nav_pkey", "t_sim_portfolio_nav", type_="primary")
    op.create_primary_key(
        "t_sim_portfolio_nav_pkey", "t_sim_portfolio_nav", ["as_of_date"]
    )

    op.drop_constraint("uq_t_sim_trades_natural", "t_sim_trades", type_="unique")
    op.create_unique_constraint(
        "uq_t_sim_trades_natural",
        "t_sim_trades",
        ["trade_date", "secCode", "side", "shares", "price"],
    )

    op.drop_constraint("t_sim_positions_pkey", "t_sim_positions", type_="primary")
    op.create_primary_key(
        "t_sim_positions_pkey", "t_sim_positions", ["as_of_date", "secCode"]
    )

    for table in ("t_sim_positions", "t_sim_trades", "t_sim_portfolio_nav"):
        op.drop_column(table, "strategy_name")
