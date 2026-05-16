"""
src/validators/schema_validator.py
JSON Schema validation + business rule checks for parsed customer records.
"""

from __future__ import annotations

import re
from typing import Any

import jsonschema
from pydantic import BaseModel, EmailStr, field_validator

from src.monitoring.logger import get_logger

log = get_logger("schema_validator")

# ── JSON Schema ───────────────────────────────────────────────────────────────

CUSTOMER_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "title": "CustomerRecord",
    "required": ["name", "email", "company"],
    "properties": {
        "crm_id":   {"type": ["string", "null"]},
        "name":     {"type": "string", "minLength": 1, "maxLength": 256},
        "email":    {
            "type": "string",
            "format": "email",
            "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
        },
        "company":  {"type": "string", "minLength": 1, "maxLength": 256},
        "tier":     {"type": ["string", "null"], "enum": ["free", "pro", "enterprise", None]},
        "phone":    {"type": ["string", "null"]},
        "address":  {"type": ["string", "null"]},
        "metadata": {"type": ["object", "null"]},
    },
    "additionalProperties": False,
}

_validator = jsonschema.Draft202012Validator(CUSTOMER_SCHEMA)


# ── Pydantic model for stricter typing ───────────────────────────────────────

class CustomerRecord(BaseModel):
    crm_id:   str | None = None
    name:     str
    email:    str
    company:  str
    tier:     str | None = None
    phone:    str | None = None
    address:  str | None = None
    metadata: dict[str, Any] | None = None

    @field_validator("email")
    @classmethod
    def normalise_email(cls, v: str) -> str:
        return v.strip().lower()

    @field_validator("tier")
    @classmethod
    def validate_tier(cls, v: str | None) -> str | None:
        if v is not None and v not in {"free", "pro", "enterprise"}:
            raise ValueError(f"Unknown tier: {v!r}")
        return v


# ── ValidationResult ─────────────────────────────────────────────────────────

class ValidationResult:
    def __init__(self, valid: bool, reason: str, record: CustomerRecord | None = None) -> None:
        self.valid = valid
        self.reason = reason
        self.record = record

    def __bool__(self) -> bool:
        return self.valid

    def __repr__(self) -> str:
        return f"ValidationResult(valid={self.valid}, reason={self.reason!r})"


# ── Validator ────────────────────────────────────────────────────────────────

class SchemaValidator:
    def validate(self, raw: dict[str, Any]) -> ValidationResult:
        """
        Run JSON Schema validation then Pydantic model coercion.
        Returns a ValidationResult; never raises.
        """
        # 1. JSON Schema structural check
        errors = sorted(_validator.iter_errors(raw), key=lambda e: e.path)
        if errors:
            reason = "; ".join(f"{'.'.join(str(p) for p in e.absolute_path)}: {e.message}" for e in errors)
            log.warning("validation_schema_failed", reason=reason)
            return ValidationResult(valid=False, reason=f"schema:{reason}")

        # 2. Pydantic coercion + field-level business rules
        try:
            record = CustomerRecord.model_validate(raw)
        except Exception as exc:
            log.warning("validation_pydantic_failed", error=str(exc))
            return ValidationResult(valid=False, reason=f"business_rule:{exc}")

        # 3. Extra business rules
        business_reason = self._business_rules(record)
        if business_reason:
            log.warning("validation_business_rule_failed", reason=business_reason)
            return ValidationResult(valid=False, reason=f"business_rule:{business_reason}")

        log.info("validation_passed", email=record.email, company=record.company)
        return ValidationResult(valid=True, reason="ok", record=record)

    # ── Business rules ────────────────────────────────────────────────────────

    @staticmethod
    def _business_rules(record: CustomerRecord) -> str | None:
        """Return a non-empty reason string if validation fails, else None."""
        # Strict email regex beyond JSON Schema's loose format check
        email_re = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
        if not email_re.match(record.email):
            return f"invalid_email_format:{record.email}"

        # Block obvious test/placeholder emails
        blocked_domains = {"example.com", "test.com", "mailinator.com", "guerrillamail.com"}
        domain = record.email.split("@")[-1].lower()
        if domain in blocked_domains:
            return f"blocked_email_domain:{domain}"

        # Name sanity
        if len(record.name.split()) < 1:
            return "name_too_short"

        return None
