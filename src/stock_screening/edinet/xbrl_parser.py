"""Arelle-based XBRL fact extraction.

Extracts JPY facts from a downloaded XBRL filing. Captures concept_id
(stable across taxonomy years) alongside the Japanese label so the
downstream mart can use concepts as the join key and avoid label-string
drift.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Categories we care about. CurrentYear* is the filer's reported period
# (vs PriorYear* for the comparative).
RELEVANT_CATEGORIES = ("CurrentYearDuration", "CurrentYearInstant")


@dataclass
class FinancialFact:
    doc_id: str
    item_name: str
    amount: float | None
    period_start: date | None
    period_end: date | None
    category_id: str
    concept_id: str
    currency_code: str = "JPY"


def _classify_unit(fact) -> str | None:
    """JPY → 'JPY', shares → 'SHR'. Anything else (USD, ratios, etc.) → None
    (signals the caller to skip this fact)."""
    if fact.unit is None:
        return None
    unit_str = str(fact.unit.value).lower()
    if "jpy" in unit_str:
        return "JPY"
    if "shares" in unit_str or "shrs" in unit_str:
        return "SHR"
    return None


def extract_facts(doc_id: str, xbrl_path: Path) -> list[FinancialFact]:
    """Load an XBRL file via Arelle and emit one FinancialFact per JPY or
    shares fact in CurrentYearDuration / CurrentYearInstant.
    """
    from arelle import Cntlr  # imported lazily; arelle is heavy

    ctrl = Cntlr.Cntlr(logFileName=None)
    try:
        model = ctrl.modelManager.load(str(xbrl_path))
        if model is None:
            logger.warning("arelle returned no model for %s", xbrl_path)
            return []

        facts: list[FinancialFact] = []
        for fact in model.facts:
            currency_code = _classify_unit(fact)
            if currency_code is None:
                continue
            if fact.contextID not in RELEVANT_CATEGORIES:
                continue

            label_ja = fact.concept.label(preferredLabel=None, lang="ja", linkroleHint=None)
            concept_id = str(fact.concept.qname) if fact.concept is not None else ""
            start = _to_date(fact.context.startDatetime)
            end = _to_date(fact.context.endDatetime)

            facts.append(
                FinancialFact(
                    doc_id=doc_id,
                    item_name=label_ja or concept_id,
                    amount=_to_float(fact.xValue),
                    period_start=start,
                    period_end=end,
                    category_id=fact.contextID,
                    concept_id=concept_id,
                    currency_code=currency_code,
                )
            )
        return facts
    finally:
        ctrl.close()


def extract_facts_many(doc_id: str, xbrl_paths: Iterable[Path]) -> list[FinancialFact]:
    """Convenience: union facts from multiple xbrl files in one filing."""
    out: list[FinancialFact] = []
    for p in xbrl_paths:
        out.extend(extract_facts(doc_id, p))
    return out


def _to_date(dt) -> date | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt.date()
    if isinstance(dt, date):
        return dt
    return None


def _to_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
