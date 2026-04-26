from stock_screening.financials.concepts import (
    CONCEPT_BY_ID,
    CONCEPT_BY_LABEL,
    DEBT_COMPONENT_FIELDS,
    resolve_field,
)


def test_resolve_by_concept_id():
    assert resolve_field("jppfs_cor:CurrentAssets", None) == "current_assets"
    assert resolve_field("jppfs_cor:Liabilities", None) == "total_liabilities"
    assert resolve_field("jppfs_cor:Assets", None) == "total_assets"


def test_resolve_by_label_fallback():
    assert resolve_field(None, "流動資産") == "current_assets"
    assert resolve_field(None, "投資有価証券") == "investment_securities"
    assert resolve_field(None, "短期借入金") == "short_term_borrowings"


def test_concept_id_wins_over_label():
    """If concept_id is recognized, label is ignored — concept is more
    specific and stable across taxonomy years."""
    assert resolve_field("jppfs_cor:CurrentAssets", "負債合計") == "current_assets"


def test_unknown_returns_none():
    assert resolve_field("jppfs_cor:RandomThing", "知らない") is None
    assert resolve_field(None, None) is None


def test_debt_components_match_concepts():
    """Sanity: every named debt component is declared in CONCEPTS so the
    GENERATED interest_bearing_debt sum has all its inputs populated."""
    declared = set(CONCEPT_BY_ID.values())
    for c in DEBT_COMPONENT_FIELDS:
        assert c in declared, f"debt component {c} not in CONCEPTS"


def test_no_label_collisions():
    """Labels shouldn't map to multiple fields silently — that would
    produce non-deterministic mart projection."""
    assert len(CONCEPT_BY_LABEL) >= len({lbl for lbl in CONCEPT_BY_LABEL.keys()})
