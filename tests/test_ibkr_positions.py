"""Parser tests for IBKR Flex OpenPositions XML — no live API calls."""

from __future__ import annotations

from xml.etree import ElementTree as ET

from stock_screening.ibkr.positions import ibkr_to_sec_code, parse_open_positions


SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="positions" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U1234567" fromDate="20260427" toDate="20260427">
      <OpenPositions>
        <OpenPosition accountId="U1234567" symbol="5363" description="TOKYO YOGYO CO LTD"
                      isin="JP3681400003" currency="JPY" position="100"
                      markPrice="2350" costBasisPrice="2230"
                      fifoPnlUnrealized="12000.0"/>
        <OpenPosition accountId="U1234567" symbol="4231" description="TIGERS POLYMER CORP"
                      isin="JP3679400000" currency="JPY" position="200"
                      markPrice="1180" costBasisPrice="950"
                      fifoPnlUnrealized="46000.0"/>
        <OpenPosition accountId="U1234567" symbol="6137" description="KOIKE SANSO KOGYO"
                      isin="JP3284000005" currency="JPY" position="50"
                      markPrice="1850" costBasisPrice="1700"
                      fifoPnlUnrealized="7500.0"/>
      </OpenPositions>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""


def test_ibkr_to_sec_code_pads_4_digit():
    assert ibkr_to_sec_code("5363") == "53630"
    assert ibkr_to_sec_code("4231") == "42310"


def test_ibkr_to_sec_code_passthrough_5_digit():
    assert ibkr_to_sec_code("53630") == "53630"


def test_ibkr_to_sec_code_handles_whitespace():
    assert ibkr_to_sec_code("  5363  ") == "53630"


def test_parse_open_positions_extracts_all_rows():
    root = ET.fromstring(SAMPLE_XML)
    positions = parse_open_positions(root)
    assert len(positions) == 3

    p = positions[0]
    assert p.symbol == "5363"
    assert p.sec_code == "53630"
    assert p.description == "TOKYO YOGYO CO LTD"
    assert p.currency == "JPY"
    assert p.quantity == 100.0
    assert p.cost_basis == 2230.0
    assert p.mark_price == 2350.0
    assert p.unrealized_pl == 12000.0
    assert p.account_id == "U1234567"
    assert p.isin == "JP3681400003"


def test_parse_open_positions_handles_missing_optional_fields():
    xml = """<?xml version="1.0"?>
    <FlexQueryResponse>
      <OpenPositions>
        <OpenPosition symbol="9999" description="X" currency="JPY" position="10"/>
      </OpenPositions>
    </FlexQueryResponse>"""
    root = ET.fromstring(xml)
    positions = parse_open_positions(root)
    assert len(positions) == 1
    p = positions[0]
    assert p.cost_basis is None
    assert p.mark_price is None
    assert p.unrealized_pl is None
    assert p.isin is None
    assert p.account_id == ""  # missing attr → empty string


def test_parse_open_positions_skips_blank_symbol():
    xml = """<?xml version="1.0"?>
    <FlexQueryResponse>
      <OpenPositions>
        <OpenPosition symbol="" position="10"/>
        <OpenPosition position="10"/>
      </OpenPositions>
    </FlexQueryResponse>"""
    root = ET.fromstring(xml)
    assert parse_open_positions(root) == []
