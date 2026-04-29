"""Parse OpenPositions out of an IBKR Flex XML response and join with
the screen mart so each holding shows current ratio + exit-trigger.

Expected Flex query setup (in IBKR portal):
  - Section: "Open Positions"
  - Default fields are fine; we read symbol/description/position/markPrice/
    costBasisPrice/currency/isin and tolerate missing optional fields.

Symbol mapping: IBKR returns 4-digit Japanese tickers (e.g., "5363"),
our DB uses 5-digit with trailing 0 (e.g., "53630"). The mapping is a
simple zfill-and-suffix.
"""

from __future__ import annotations

from dataclasses import dataclass
from xml.etree import ElementTree as ET

import pandas as pd
from sqlalchemy import text


@dataclass(frozen=True)
class Position:
    account_id: str
    symbol: str            # IBKR's symbol as reported (e.g. "5363")
    sec_code: str          # 5-digit DB code (e.g. "53630")
    description: str
    isin: str | None
    currency: str
    quantity: float
    cost_basis: float | None       # per-share
    mark_price: float | None
    unrealized_pl: float | None


def ibkr_to_sec_code(symbol: str) -> str:
    """IBKR ticker → 5-digit DB code.

    IBKR returns Japanese symbols as e.g. "5363.T" (TSE suffix). Strip
    the exchange suffix and pad 4-digit tickers with a trailing 0.
    """
    s = symbol.strip()
    # Strip exchange suffix after a dot (e.g. ".T" for TSE).
    if "." in s:
        s = s.split(".", 1)[0]
    if len(s) == 4 and s.isdigit():
        return s + "0"
    return s


def _f(s: str | None) -> float | None:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_open_positions(root: ET.Element) -> list[Position]:
    """Walk the Flex XML and yield Position rows. Tolerates the common
    structures (FlexQueryResponse → FlexStatements → FlexStatement →
    OpenPositions → OpenPosition)."""
    positions: list[Position] = []
    for op in root.iter("OpenPosition"):
        symbol = (op.attrib.get("symbol") or "").strip()
        if not symbol:
            continue
        positions.append(
            Position(
                account_id=op.attrib.get("accountId", ""),
                symbol=symbol,
                sec_code=ibkr_to_sec_code(symbol),
                description=op.attrib.get("description", ""),
                isin=op.attrib.get("isin") or None,
                currency=op.attrib.get("currency", ""),
                quantity=float(op.attrib.get("position", "0") or 0),
                cost_basis=_f(op.attrib.get("costBasisPrice")),
                mark_price=_f(op.attrib.get("markPrice")),
                unrealized_pl=_f(op.attrib.get("fifoPnlUnrealized")),
            )
        )
    return positions


def enrich_with_screen(engine, positions: list[Position]) -> pd.DataFrame:
    """Join positions with the latest screen metrics so each holding
    shows ratio / PER / op_yield / momentum / Q+T flags / exit-trigger
    state. Stocks not in our DB get NaN columns and are flagged."""
    if not positions:
        return pd.DataFrame()
    pos_df = pd.DataFrame([p.__dict__ for p in positions])
    codes = pos_df["sec_code"].tolist()

    HAIRCUT = 0.7
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH visible AS (
                    SELECT fa."secCode" sec_code, fa.period_end,
                           fa.current_assets, fa.total_liabilities,
                           fa.investment_securities, fa.operating_income,
                           fa.net_sales, fa.net_income, fa.issued_shares,
                           fa.source_doc_id
                    FROM t_financials_annual fa
                    JOIN t_doc_list dl ON dl."docID" = fa.source_doc_id
                    WHERE dl."submitDateTime"::date <= CURRENT_DATE
                      AND fa.current_assets IS NOT NULL
                      AND fa."secCode" = ANY(:codes)
                ),
                latest AS (
                    SELECT DISTINCT ON (sec_code) * FROM visible
                    ORDER BY sec_code, period_end DESC
                ),
                ranked AS (
                    SELECT fa2."secCode", fa2.period_end,
                           fa2.operating_income, fa2.net_sales,
                           LAG(fa2.operating_income) OVER w op_prev,
                           LAG(fa2.net_sales)        OVER w sales_prev
                    FROM t_financials_annual fa2
                    JOIN t_doc_list dl2 ON dl2."docID"=fa2.source_doc_id
                    WHERE dl2."submitDateTime"::date <= CURRENT_DATE
                      AND fa2."secCode" = ANY(:codes)
                    WINDOW w AS (PARTITION BY fa2."secCode" ORDER BY fa2.period_end)
                ),
                yoy AS (
                    SELECT DISTINCT ON ("secCode") "secCode" sec_code, op_prev, sales_prev
                    FROM ranked WHERE op_prev IS NOT NULL OR sales_prev IS NOT NULL
                    ORDER BY "secCode", period_end DESC
                )
                SELECT l.sec_code, l.current_assets, l.total_liabilities,
                       l.investment_securities, l.operating_income,
                       l.net_sales, l.net_income, l.issued_shares,
                       l.period_end,
                       y.op_prev, y.sales_prev,
                       d.adj_close db_price, d."marketCap" mc,
                       d6.adj_close p_6mo
                FROM latest l
                JOIN LATERAL (
                    SELECT adj_close, "marketCap" FROM t_daily_stock_perf p
                    WHERE p."ShokenCode" = l.sec_code
                      AND p.adj_close IS NOT NULL AND p."marketCap" IS NOT NULL
                    ORDER BY p."Date" DESC LIMIT 1
                ) d ON TRUE
                LEFT JOIN LATERAL (
                    SELECT adj_close FROM t_daily_stock_perf p
                    WHERE p."ShokenCode" = l.sec_code
                      AND p."Date" <= CURRENT_DATE - INTERVAL '160 days'
                      AND p."Date" >= CURRENT_DATE - INTERVAL '200 days'
                      AND p.adj_close IS NOT NULL
                    ORDER BY p."Date" DESC LIMIT 1
                ) d6 ON TRUE
                LEFT JOIN yoy y ON y.sec_code = l.sec_code
                """
            ),
            {"codes": codes},
        ).fetchall()

    cols = ["sec_code", "current_assets", "total_liabilities",
            "investment_securities", "operating_income", "net_sales", "net_income",
            "issued_shares", "period_end", "op_prev", "sales_prev",
            "db_price", "mc", "p_6mo"]
    fund = pd.DataFrame(rows, columns=cols)
    for c in [c for c in cols if c not in ("sec_code", "period_end")]:
        fund[c] = pd.to_numeric(fund[c], errors="coerce")

    fund["ratio"] = (
        fund["current_assets"] - fund["total_liabilities"]
        + HAIRCUT * fund["investment_securities"].fillna(0)
    ) / fund["mc"]
    fund["per"] = fund["mc"] / fund["net_income"]
    fund["op_yield"] = fund["operating_income"] / fund["mc"]
    fund["op_yoy"] = (fund["operating_income"] - fund["op_prev"]) / fund["op_prev"].abs()
    fund["sales_yoy"] = (fund["net_sales"] - fund["sales_prev"]) / fund["sales_prev"]
    fund["mom_6m"] = fund["db_price"] / fund["p_6mo"] - 1
    fund["q_flag"] = (fund["op_yield"] > 0.05) & (fund["mom_6m"] > 0)
    fund["t_flag"] = (fund["op_yoy"] > 0.20) & (fund["sales_yoy"] > 0)

    merged = pos_df.merge(fund, on="sec_code", how="left")
    merged["exit_triggered"] = merged["ratio"] < 1.0
    return merged
