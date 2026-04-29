"""Pure-logic tests for the trade plan generator (no DB / no API)."""

from __future__ import annotations

import pandas as pd

from importlib.machinery import SourceFileLoader
from pathlib import Path

# Import the script as a module so we can test its helpers.
_path = Path(__file__).resolve().parents[1] / "scripts" / "generate_trade_plan.py"
trade_plan = SourceFileLoader("trade_plan", str(_path)).load_module()


def test_limit_price_rounds_up_with_buffer():
    # 1317 * 1.005 = 1323.585 → 1324
    assert trade_plan.limit_price(1317.0) == 1324
    # 526 * 1.005 = 528.63 → 529
    assert trade_plan.limit_price(526.0) == 529
    # 932 * 1.005 = 936.66 → 937
    assert trade_plan.limit_price(932.0) == 937


def test_best_deployment_picks_highest_flag_count_within_budget():
    # 3 candidates: A is Q+T but expensive; B is Q+T and affordable; C is Q only.
    # Should pick B (top flag count, fits budget).
    targets = pd.DataFrame([
        {"sec_code": "A", "name": "A Inc", "price_now": 2000.0, "ratio": 4.0,
         "per": 5.0, "q_flag": True, "t_flag": True},
        {"sec_code": "B", "name": "B Inc", "price_now": 1300.0, "ratio": 3.0,
         "per": 4.0, "q_flag": True, "t_flag": True},
        {"sec_code": "C", "name": "C Inc", "price_now": 900.0, "ratio": 5.0,
         "per": 3.0, "q_flag": True, "t_flag": False},
    ])
    plan = trade_plan.best_deployment(135_000, targets, current_codes=set())
    assert plan is not None
    assert plan["sec_code"] == "B"


def test_best_deployment_skips_already_held():
    targets = pd.DataFrame([
        {"sec_code": "A", "name": "A Inc", "price_now": 1000.0, "ratio": 5.0,
         "per": 4.0, "q_flag": True, "t_flag": True},
        {"sec_code": "B", "name": "B Inc", "price_now": 1100.0, "ratio": 3.0,
         "per": 4.0, "q_flag": True, "t_flag": False},
    ])
    plan = trade_plan.best_deployment(150_000, targets, current_codes={"A"})
    assert plan is not None
    assert plan["sec_code"] == "B"


def test_best_deployment_returns_none_when_budget_too_small():
    targets = pd.DataFrame([
        {"sec_code": "A", "name": "A Inc", "price_now": 5000.0, "ratio": 3.0,
         "per": 5.0, "q_flag": True, "t_flag": True},
    ])
    plan = trade_plan.best_deployment(100_000, targets, current_codes=set())
    assert plan is None


def test_best_deployment_tiebreak_by_ratio_then_flag_count():
    # Two Q+T picks both fit — higher ratio wins.
    targets = pd.DataFrame([
        {"sec_code": "A", "name": "A Inc", "price_now": 1000.0, "ratio": 2.0,
         "per": 5.0, "q_flag": True, "t_flag": True},
        {"sec_code": "B", "name": "B Inc", "price_now": 1100.0, "ratio": 5.0,
         "per": 4.0, "q_flag": True, "t_flag": True},
    ])
    plan = trade_plan.best_deployment(200_000, targets, current_codes=set())
    assert plan is not None
    assert plan["sec_code"] == "B"


def test_best_deployment_returns_none_when_all_held():
    targets = pd.DataFrame([
        {"sec_code": "A", "name": "A", "price_now": 1000.0, "ratio": 3.0,
         "per": 5.0, "q_flag": True, "t_flag": True},
    ])
    plan = trade_plan.best_deployment(200_000, targets, current_codes={"A"})
    assert plan is None
