"""Portfolio state: load/save positions, trades, NAV; mark-to-market."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .rebalance import Trade


@dataclass
class PortfolioSnapshot:
    as_of_date: date
    cash: float
    positions: pd.DataFrame  # sec_code, shares, avg_cost, last_price, market_value
    total_nav: float

    @property
    def positions_value(self) -> float:
        if self.positions.empty:
            return 0.0
        return float(self.positions["market_value"].sum())

    def shares_dict(self) -> dict[str, int]:
        if self.positions.empty:
            return {}
        return {row.secCode: int(row.shares) for row in self.positions.itertuples()}


def delete_outputs_for_date(engine: Engine, run_date: date) -> None:
    """Idempotency hook for backfill / re-runs."""
    with engine.begin() as conn:
        for table, col in (
            ("t_sim_positions", "as_of_date"),
            ("t_sim_trades", "trade_date"),
            ("t_sim_portfolio_nav", "as_of_date"),
        ):
            conn.execute(text(f"DELETE FROM {table} WHERE {col} = :d"), {"d": run_date})


def load_previous_snapshot(engine: Engine, before_date: date) -> PortfolioSnapshot | None:
    """Most recent NAV snapshot strictly before `before_date`. Returns
    None on a cold start (no prior runs).
    """
    with engine.connect() as conn:
        nav_row = conn.execute(
            text(
                "SELECT as_of_date, cash, positions_value, total_nav, n_positions "
                "FROM t_sim_portfolio_nav WHERE as_of_date < :d "
                "ORDER BY as_of_date DESC LIMIT 1"
            ),
            {"d": before_date},
        ).mappings().first()
        if nav_row is None:
            return None

        positions = pd.read_sql(
            text(
                'SELECT "secCode", shares, avg_cost, last_price, market_value '
                "FROM t_sim_positions WHERE as_of_date = :d"
            ),
            conn,
            params={"d": nav_row["as_of_date"]},
        )

    return PortfolioSnapshot(
        as_of_date=nav_row["as_of_date"],
        cash=float(nav_row["cash"]),
        positions=positions,
        total_nav=float(nav_row["total_nav"]),
    )


def fetch_prices(engine: Engine, run_date: date, sec_codes: list[str]) -> dict[str, float]:
    if not sec_codes:
        return {}
    sql = text(
        'SELECT "ShokenCode" AS sec_code, close '
        "FROM t_daily_stock_perf "
        'WHERE "Date" = :d AND "ShokenCode" = ANY(:codes) '
        "AND close IS NOT NULL"
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"d": run_date, "codes": sec_codes}).all()
    return {r.sec_code: float(r.close) for r in rows}


def mark_to_market(positions: pd.DataFrame, prices: dict[str, float]) -> pd.DataFrame:
    """Refresh last_price and market_value. Names with no price keep
    yesterday's last_price (stale-mark).
    """
    if positions.empty:
        return positions

    out = positions.copy()
    out["last_price"] = out.apply(
        lambda r: prices.get(r["secCode"], r["last_price"]), axis=1
    )
    out["market_value"] = out["shares"] * out["last_price"]
    return out


def apply_trades(
    positions: pd.DataFrame, trades: list[Trade], cash: float
) -> tuple[pd.DataFrame, float]:
    """Update positions and cash by walking trades. Sells first (per
    `generate_trades`'s ordering); buys consume the freed cash.
    """
    pos = positions.set_index("secCode") if not positions.empty else pd.DataFrame(
        columns=["shares", "avg_cost", "last_price", "market_value"]
    ).rename_axis("secCode")

    new_cash = cash
    for t in trades:
        if t.side == "S":
            held = int(pos.loc[t.sec_code, "shares"]) if t.sec_code in pos.index else 0
            new_held = held - t.shares
            new_cash += t.notional() - t.commission
            if new_held == 0:
                pos = pos.drop(t.sec_code, errors="ignore")
            else:
                pos.loc[t.sec_code, "shares"] = new_held
                pos.loc[t.sec_code, "last_price"] = t.price
                pos.loc[t.sec_code, "market_value"] = new_held * t.price
        else:  # 'B'
            held = int(pos.loc[t.sec_code, "shares"]) if t.sec_code in pos.index else 0
            old_cost = float(pos.loc[t.sec_code, "avg_cost"]) if t.sec_code in pos.index else 0.0
            new_held = held + t.shares
            new_avg = ((held * old_cost) + (t.shares * t.price)) / new_held if new_held else 0.0
            new_cash -= t.notional() + t.commission
            pos.loc[t.sec_code, "shares"] = new_held
            pos.loc[t.sec_code, "avg_cost"] = new_avg
            pos.loc[t.sec_code, "last_price"] = t.price
            pos.loc[t.sec_code, "market_value"] = new_held * t.price

    return pos.reset_index(), new_cash


def persist_positions(engine: Engine, as_of_date: date, positions: pd.DataFrame) -> int:
    if positions.empty:
        return 0
    rows = [
        {
            "as_of_date": as_of_date,
            "secCode": r["secCode"],
            "shares": int(r["shares"]),
            "avg_cost": float(r["avg_cost"]),
            "last_price": float(r["last_price"]),
            "market_value": float(r["market_value"]),
        }
        for _, r in positions.iterrows()
    ]
    sql = text(
        """
        INSERT INTO t_sim_positions
            (as_of_date, "secCode", shares, avg_cost, last_price, market_value)
        VALUES (:as_of_date, :secCode, :shares, :avg_cost, :last_price, :market_value)
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def persist_trades(engine: Engine, trade_date: date, trades: list[Trade]) -> int:
    if not trades:
        return 0
    rows = [
        {
            "trade_date": trade_date,
            "secCode": t.sec_code,
            "side": t.side,
            "shares": t.shares,
            "price": t.price,
            "commission": t.commission,
            "reason": t.reason,
        }
        for t in trades
    ]
    sql = text(
        """
        INSERT INTO t_sim_trades
            (trade_date, "secCode", side, shares, price, commission, reason)
        VALUES (:trade_date, :secCode, :side, :shares, :price, :commission, :reason)
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def persist_nav(
    engine: Engine,
    as_of_date: date,
    cash: float,
    positions: pd.DataFrame,
    benchmark_nav: float | None,
) -> None:
    pos_value = float(positions["market_value"].sum()) if not positions.empty else 0.0
    n = int(len(positions))
    sql = text(
        """
        INSERT INTO t_sim_portfolio_nav
            (as_of_date, cash, positions_value, total_nav, benchmark_nav, n_positions)
        VALUES (:as_of_date, :cash, :positions_value, :total_nav, :benchmark_nav, :n)
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "as_of_date": as_of_date,
                "cash": cash,
                "positions_value": pos_value,
                "total_nav": cash + pos_value,
                "benchmark_nav": benchmark_nav,
                "n": n,
            },
        )


def previous_business_date(engine: Engine, before: date) -> date | None:
    """Most recent date with a NAV snapshot strictly before `before`."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT as_of_date FROM t_sim_portfolio_nav "
                "WHERE as_of_date < :d ORDER BY as_of_date DESC LIMIT 1"
            ),
            {"d": before},
        ).first()
    return row[0] if row else None


def benchmark_nav_for(
    engine: Engine,
    sim_start_date: date,
    run_date: date,
    initial_capital: float,
    benchmark_ticker: str,
) -> float | None:
    """Buy-and-hold of benchmark from sim start to run_date.

    Reads start and current close from t_daily_stock_perf. Returns None
    if either close is missing (e.g. ticker not yet in DB on first run).
    """
    sql = text(
        'SELECT "Date" AS d, close FROM t_daily_stock_perf '
        'WHERE "ShokenCode" = :code AND "Date" IN (:start, :now)'
    )
    with engine.connect() as conn:
        rows = {r.d: float(r.close) for r in conn.execute(
            sql, {"code": benchmark_ticker, "start": sim_start_date, "now": run_date}
        ).all() if r.close is not None}
    if sim_start_date not in rows or run_date not in rows or rows[sim_start_date] == 0:
        return None
    return initial_capital * (rows[run_date] / rows[sim_start_date])


def cold_start_date(target: date) -> date:
    """The 'previous business date' for a fresh sim — just yesterday;
    sim DAG bootstraps from today's qualifying names if no prior snapshot.
    """
    return target - timedelta(days=1)
