"""
src/crm_client.py
Thin adapter for the legacy CRM undocumented REST API.
All HTTP calls go through this class — resilience logic lives in the orchestrator.
"""

from __future__ import annotations

import os
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.monitoring.logger import get_logger

log = get_logger("crm_client")

CRM_BASE_URL = os.environ.get("CRM_BASE_URL", "https://legacy-crm.corp/api/v1")
CRM_API_KEY  = os.environ.get("CRM_API_KEY", "")
REQUEST_TIMEOUT = 15  # seconds


class CRMClient:
    """
    Wraps the legacy CRM API.

    Endpoints used:
      GET  /customers?email=<email>         → look up existing CRM ID by email
      POST /customers                       → create new customer
      PATCH /customers/<crm_id>             → update existing customer

    The underlying requests Session uses urllib3's low-level retry for
    connection-level failures; application-level retries (429, 5xx) are
    handled upstream by the orchestrator's retry_handler + circuit_breaker.
    """

    def __init__(self, base_url: str = CRM_BASE_URL, api_key: str = CRM_API_KEY) -> None:
        self._base = base_url.rstrip("/")
        self._session = self._build_session(api_key)

    # ── Public API ────────────────────────────────────────────────────────────

    def lookup_by_email(self, email: str) -> str | None:
        """Return the CRM ID for an existing customer, or None if not found."""
        resp = self._session.get(
            f"{self._base}/customers",
            params={"email": email},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        records: list[dict] = data.get("results", []) if isinstance(data, dict) else data
        return records[0].get("id") if records else None

    def create_customer(self, payload: dict[str, Any]) -> dict[str, Any]:
        """POST a new customer record. Returns the created record."""
        resp = self._session.post(
            f"{self._base}/customers",
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def update_customer(self, crm_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        """PATCH an existing customer record. Returns the updated record."""
        resp = self._session.patch(
            f"{self._base}/customers/{crm_id}",
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def upsert(self, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """
        Upsert a customer record.

        Priority order for CRM ID resolution:
          1. crm_id field already in payload
          2. Lookup by email
          3. Create new record

        Returns:
          (action, response) where action is "created" | "updated"
        """
        crm_id: str | None = payload.get("crm_id") or self.lookup_by_email(payload["email"])

        if crm_id:
            log.info("crm_upsert_update", crm_id=crm_id, email=payload["email"])
            result = self.update_customer(crm_id, payload)
            return "updated", result
        else:
            log.info("crm_upsert_create", email=payload["email"])
            result = self.create_customer(payload)
            return "created", result

    # ── Session setup ─────────────────────────────────────────────────────────

    @staticmethod
    def _build_session(api_key: str) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "EnterpriseOnboardingAgent/1.0",
            }
        )
        # Low-level urllib3 retry: only for connect/read timeouts and 503
        # Application-level 429/5xx retry is handled upstream.
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=2,
                status_forcelist=[503],
                backoff_factor=0.5,
                allowed_methods={"GET", "POST", "PATCH"},
            )
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
