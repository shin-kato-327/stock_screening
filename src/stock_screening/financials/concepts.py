"""XBRL concept ID mappings for the financial mart.

The annual mart projects raw EAV `t_financials` rows into typed columns
via the concept_id (stable across taxonomy years). For legacy rows
ingested before the parser captured concept_id, fall back to matching
the Japanese label (item_name).

Concept IDs below are the common forms in the Japan GAAP corporate
disclosure taxonomy (jpcrp_cor / jppfs_cor). Verify against a real
filing during the first end-to-end run; if any company reports a
metric under a non-standard concept, add it to the labels fallback.

The screening formula uses:
  net_cash_ratio = (current_assets - interest_bearing_debt + 0.7*investment_securities) / market_cap
  interest_bearing_debt = short_term_borrowings + long_term_borrowings + bonds + lease_obligations
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ConceptMap:
    field_name: str
    # Primary XBRL concept IDs (most filings).
    concept_ids: tuple[str, ...]
    # Japanese label fallbacks (legacy rows or unusual filings).
    labels: tuple[str, ...] = field(default_factory=tuple)


CONCEPTS: tuple[ConceptMap, ...] = (
    ConceptMap(
        field_name="current_assets",
        concept_ids=("jppfs_cor:CurrentAssets",),
        labels=("流動資産",),
    ),
    ConceptMap(
        field_name="short_term_borrowings",
        concept_ids=(
            "jppfs_cor:ShortTermLoansPayable",
            "jppfs_cor:ShortTermBorrowings",
        ),
        labels=("短期借入金",),
    ),
    ConceptMap(
        field_name="long_term_borrowings",
        concept_ids=(
            "jppfs_cor:LongTermLoansPayable",
            "jppfs_cor:LongTermBorrowings",
        ),
        labels=("長期借入金",),
    ),
    ConceptMap(
        field_name="bonds",
        concept_ids=(
            "jppfs_cor:BondsPayable",
            "jppfs_cor:CurrentPortionOfBonds",
        ),
        labels=("社債", "1年内償還予定の社債"),
    ),
    ConceptMap(
        field_name="lease_obligations",
        concept_ids=(
            "jppfs_cor:LeaseObligationsCL",
            "jppfs_cor:LeaseObligationsNCL",
        ),
        labels=("リース債務",),
    ),
    ConceptMap(
        field_name="investment_securities",
        concept_ids=("jppfs_cor:InvestmentSecurities",),
        labels=("投資有価証券",),
    ),
    ConceptMap(
        field_name="total_liabilities",
        concept_ids=(
            "jppfs_cor:Liabilities",
            "jpcrp_cor:LiabilitiesSummaryOfBusinessResults",
        ),
        labels=("負債合計",),
    ),
    ConceptMap(
        field_name="total_assets",
        # The Summary form reports the consolidated total in the 5-year
        # highlights table; many filings emit it instead of (or in addition
        # to) jppfs_cor:Assets in the consolidated balance sheet.
        concept_ids=(
            "jppfs_cor:Assets",
            "jpcrp_cor:TotalAssetsSummaryOfBusinessResults",
        ),
        labels=("資産合計", "総資産額"),
    ),
    ConceptMap(
        field_name="issued_shares",
        # Verified against real filings — this is the concept used in
        # 有報's "shares issued / voting rights" section. Total issued
        # shares ≈ voting-rights shares for the vast majority of TSE
        # filers (companies with large non-voting tranches are rare).
        concept_ids=(
            "jpcrp_cor:NumberOfSharesIssuedSharesVotingRights",
        ),
        labels=("株式数（株）", "発行済株式総数"),
    ),
)

# Index for fast resolution at mart-build time.
CONCEPT_BY_ID: dict[str, str] = {
    cid: cm.field_name for cm in CONCEPTS for cid in cm.concept_ids
}
CONCEPT_BY_LABEL: dict[str, str] = {
    lbl: cm.field_name for cm in CONCEPTS for lbl in cm.labels
}

# Components that get summed into interest_bearing_debt by the GENERATED
# STORED column in t_financials_annual.
DEBT_COMPONENT_FIELDS = (
    "short_term_borrowings",
    "long_term_borrowings",
    "bonds",
    "lease_obligations",
)


def resolve_field(concept_id: str | None, item_name: str | None) -> str | None:
    """Return the t_financials_annual field name for a raw fact, or None
    if the fact isn't one of the screen inputs.
    """
    if concept_id and concept_id in CONCEPT_BY_ID:
        return CONCEPT_BY_ID[concept_id]
    if item_name and item_name in CONCEPT_BY_LABEL:
        return CONCEPT_BY_LABEL[item_name]
    return None
