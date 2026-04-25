"""J-Quants API client. Refresh-token in → ID token cached in process.

Auth flow (paid tier): refresh token (issued via portal, ~7 days)
→ POST /v1/token/auth_refresh → idToken (~24h, cached here) →
Authorization: Bearer for all data endpoints.
"""

from __future__ import annotations

import logging
import time
from datetime import date

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

BASE = "https://api.jquants.com/v1"

# Markets we screen — Prime + Standard.
PRIME_MARKET = "0111"
STANDARD_MARKET = "0112"

ID_TOKEN_TTL_SECONDS = 23 * 3600  # refresh a bit before the 24h expiry

logger = logging.getLogger(__name__)


class JQuantsClient:
    def __init__(self, refresh_token: str):
        self._refresh_token = refresh_token
        self._id_token: str | None = None
        self._id_token_expires_at: float = 0.0

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _refresh_id_token(self) -> str:
        resp = requests.post(
            f"{BASE}/token/auth_refresh",
            params={"refreshtoken": self._refresh_token},
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json()["idToken"]
        self._id_token = token
        self._id_token_expires_at = time.time() + ID_TOKEN_TTL_SECONDS
        return token

    def _ensure_id_token(self) -> str:
        if self._id_token and time.time() < self._id_token_expires_at:
            return self._id_token
        return self._refresh_id_token()

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._ensure_id_token()}"}

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def listed_info(self, target_date: date | None = None) -> pd.DataFrame:
        """Listed company info. Filter Prime/Standard via MarketCode."""
        params = {}
        if target_date is not None:
            params["date"] = target_date.strftime("%Y%m%d")
        resp = requests.get(
            f"{BASE}/listed/info", headers=self._headers(), params=params, timeout=60
        )
        resp.raise_for_status()
        return pd.DataFrame(resp.json().get("info", []))

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
    ) -> pd.DataFrame:
        """Daily quotes. Either date (all stocks that day) or code (one
        stock's history). Returns columns including Code, Date, Close,
        Volume, MarketCapitalization, etc.
        """
        params = {}
        if target_date is not None:
            params["date"] = target_date.strftime("%Y%m%d")
        if code is not None:
            params["code"] = code
        all_rows: list[dict] = []
        pagination_key: str | None = None
        while True:
            if pagination_key:
                params["pagination_key"] = pagination_key
            resp = requests.get(
                f"{BASE}/prices/daily_quotes",
                headers=self._headers(),
                params=params,
                timeout=60,
            )
            resp.raise_for_status()
            body = resp.json()
            all_rows.extend(body.get("daily_quotes", []))
            pagination_key = body.get("pagination_key")
            if not pagination_key:
                break
        return pd.DataFrame(all_rows)
