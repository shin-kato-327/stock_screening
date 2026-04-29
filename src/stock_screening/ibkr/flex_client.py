"""IBKR Flex Web Service client.

Two-step protocol:
  1. SendRequest with (token, query_id) → returns ReferenceCode
  2. GetStatement with (token, ref_code) → returns the actual XML report

Step 2 may return a "still generating" warning (ErrorCode 1019); the
client polls until the report is ready or the timeout elapses.

Read-only — Flex queries can only return reports the user pre-defined
in their IBKR account portal. The token is account-scoped and revocable
without affecting trading credentials.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from xml.etree import ElementTree as ET

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

BASE = "https://gdcdyn.interactivebrokers.com/Universal/servlet"
SEND_PATH = f"{BASE}/FlexStatementService.SendRequest"
GET_PATH = f"{BASE}/FlexStatementService.GetStatement"
API_VERSION = "3"

# IBKR's "still generating, retry" code.
STILL_GENERATING_CODE = "1019"

logger = logging.getLogger(__name__)


class FlexAPIError(RuntimeError):
    pass


@dataclass(frozen=True)
class FlexResponse:
    """Raw XML body of a successful GetStatement call."""
    xml: str
    root: ET.Element


class FlexClient:
    def __init__(self, token: str, *, poll_interval_s: float = 3.0,
                 poll_timeout_s: float = 120.0):
        self._token = token
        self._poll_interval_s = poll_interval_s
        self._poll_timeout_s = poll_timeout_s

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=20),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _send(self, query_id: str) -> str:
        params = {"t": self._token, "q": query_id, "v": API_VERSION}
        r = requests.get(SEND_PATH, params=params, timeout=30)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        status = (root.findtext("Status") or "").strip()
        if status != "Success":
            err_code = root.findtext("ErrorCode") or "?"
            err_msg = root.findtext("ErrorMessage") or "(no message)"
            raise FlexAPIError(
                f"SendRequest returned {status}: {err_code} {err_msg}"
            )
        ref = root.findtext("ReferenceCode")
        if not ref:
            raise FlexAPIError(f"SendRequest missing ReferenceCode: {r.text[:200]}")
        return ref

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=20),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _get(self, ref_code: str) -> requests.Response:
        params = {"t": self._token, "q": ref_code, "v": API_VERSION}
        r = requests.get(GET_PATH, params=params, timeout=60)
        r.raise_for_status()
        return r

    def fetch_query(self, query_id: str) -> FlexResponse:
        """End-to-end: send + poll + return parsed XML root."""
        ref = self._send(query_id)
        logger.info("flex query %s submitted, ref=%s", query_id, ref)
        deadline = time.monotonic() + self._poll_timeout_s
        while time.monotonic() < deadline:
            r = self._get(ref)
            # The "still generating" response is itself a small
            # FlexStatementResponse with Status=Warn. The success response
            # is the full FlexQueryResponse with the actual data.
            try:
                root = ET.fromstring(r.text)
            except ET.ParseError as e:
                raise FlexAPIError(f"GetStatement returned non-XML: {e}: {r.text[:200]}")
            if root.tag == "FlexStatementResponse":
                status = (root.findtext("Status") or "").strip()
                err_code = root.findtext("ErrorCode") or "?"
                err_msg = root.findtext("ErrorMessage") or ""
                if err_code == STILL_GENERATING_CODE:
                    logger.debug("statement still generating, sleeping %ss", self._poll_interval_s)
                    time.sleep(self._poll_interval_s)
                    continue
                raise FlexAPIError(
                    f"GetStatement returned {status}: {err_code} {err_msg}"
                )
            return FlexResponse(xml=r.text, root=root)
        raise FlexAPIError(
            f"GetStatement timed out after {self._poll_timeout_s}s waiting for ref {ref}"
        )
