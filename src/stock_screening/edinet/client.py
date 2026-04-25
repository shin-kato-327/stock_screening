"""EDINET API v2 client — document list + XBRL bundle download."""

from __future__ import annotations

import io
import logging
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

LIST_URL = "https://disclosure.edinet-fsa.go.jp/api/v2/documents.json"
DOC_URL_TMPL = "https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"

# Securities reports (有価証券報告書)
SECURITIES_REPORT_FORM_CODE = "030000"
SECURITIES_REPORT_ORDINANCE_CODE = "010"

DOC_LIST_COLUMNS = (
    "docID",
    "edinetCode",
    "secCode",
    "JCN",
    "filerName",
    "fundCode",
    "ordinanceCode",
    "formCode",
    "docTypeCode",
    "periodStart",
    "periodEnd",
    "submitDateTime",
    "docDescription",
    "issuerEdinetCode",
    "subjectEdinetCode",
    "currentReportReason",
    "parentDocID",
    "opeDateTime",
    "xbrlFlag",
    "pdfFlag",
    "csvFlag",
)

logger = logging.getLogger(__name__)


@dataclass
class DocListEntry:
    raw: dict

    @property
    def doc_id(self) -> str:
        return self.raw["docID"]

    @property
    def is_securities_report(self) -> bool:
        return (
            self.raw.get("formCode") == SECURITIES_REPORT_FORM_CODE
            and self.raw.get("ordinanceCode") == SECURITIES_REPORT_ORDINANCE_CODE
        )

    def to_row(self) -> dict:
        return {col: self.raw.get(col) for col in DOC_LIST_COLUMNS}


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def list_documents(target_date: date, edinet_key: str) -> list[DocListEntry]:
    """Fetch the document list for a single date from EDINET v2.

    `type=2` returns the metadata for filings submitted on `target_date`
    (not the documents themselves). Caller is responsible for filtering
    to securities reports if desired (use `entry.is_securities_report`).
    """
    params = {
        "date": target_date.isoformat(),
        "type": 2,
        "Subscription-Key": edinet_key,
    }
    resp = requests.get(LIST_URL, params=params, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    status = body.get("metadata", {}).get("status")
    if status != "200":
        raise RuntimeError(f"EDINET returned status {status} for {target_date}")
    return [DocListEntry(raw=r) for r in body.get("results", [])]


def filter_securities_reports(entries: Iterable[DocListEntry]) -> list[DocListEntry]:
    return [e for e in entries if e.is_securities_report]


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def download_xbrl_bundle(doc_id: str, edinet_key: str, dest_dir: Path) -> list[Path]:
    """Download the XBRL+XSD bundle for a doc, extract to dest_dir.

    Returns the list of extracted file paths (only .xbrl and .xsd
    under XBRL/PublicDoc/, matching the pattern used by the parser).
    """
    url = DOC_URL_TMPL.format(doc_id=doc_id)
    params = {"type": 1, "Subscription-Key": edinet_key}
    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()

    dest_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for name in zf.namelist():
            if name.startswith("XBRL/PublicDoc/") and name.endswith((".xbrl", ".xsd")):
                target = dest_dir / name
                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(name) as src, open(target, "wb") as out:
                    out.write(src.read())
                extracted.append(target)
    if not extracted:
        logger.warning("no XBRL/XSD files found in bundle for %s", doc_id)
    return extracted
