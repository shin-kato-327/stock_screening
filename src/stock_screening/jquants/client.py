"""J-Quants v2 API client.

The v2 API uses a single x-api-key header. No JWT refresh flow — simpler
than v1.

Endpoints used:
- /v2/equities/bars/daily — daily OHLC + volume per stock
"""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

BASE = "https://api.jquants.com/v2"

# v1 had MarketCode 0111 (Prime) / 0112 (Standard). v2 may differ; the
# screen falls back to "everything daily_quotes returns" if listed-info
# isn't available.
PRIME_MARKET = "0111"
STANDARD_MARKET = "0112"

logger = logging.getLogger(__name__)


class JQuantsClient:
    def __init__(self, api_key: str):
        self._api_key = api_key

    def _headers(self) -> dict:
        return {"x-api-key": self._api_key}

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def daily_quotes(
        self,
        target_date: date | None = None,
        code: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> pd.DataFrame:
        """GET /v2/equities/bars/daily.

        Either `target_date` (all stocks that day) or `code` (one stock,
        optionally with from/to) must be set. Returns a DataFrame with
        columns Date, Code, O/H/L/C, Vo, Va, AdjFactor, AdjO/H/L/C, AdjVo.
        """
        params = {}
        if target_date is not None:
            params["date"] = target_date.strftime("%Y%m%d")
        if code is not None:
            params["code"] = code
        if date_from is not None:
            params["from"] = date_from.strftime("%Y%m%d")
        if date_to is not None:
            params["to"] = date_to.strftime("%Y%m%d")

        all_rows: list[dict] = []
        pagination_key: str | None = None
        while True:
            if pagination_key:
                params["pagination_key"] = pagination_key
            resp = requests.get(
                f"{BASE}/equities/bars/daily",
                headers=self._headers(),
                params=params,
                timeout=60,
            )
            if not resp.ok:
                # Don't echo headers/body — could include the api key on retry mishaps.
                raise requests.HTTPError(
                    f"jquants daily_quotes returned {resp.status_code}: {resp.text[:200]}"
                )
            body = resp.json()
            all_rows.extend(body.get("data", []))
            pagination_key = body.get("pagination_key")
            if not pagination_key:
                break
        return pd.DataFrame(all_rows)
