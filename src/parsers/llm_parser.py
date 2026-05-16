"""
src/parsers/llm_parser.py

Claude-powered extraction of structured customer records from raw text chunks.
Implements a self-correction loop (max 2 rounds) to handle malformed JSON output
caused by OCR noise, truncation, or mixed-language content.
"""

from __future__ import annotations

import json
import os
from typing import Any

import anthropic

from src.monitoring.logger import get_logger

log = get_logger("llm_parser")

MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 1024
MAX_SELF_CORRECTIONS = 2

# ── Prompt templates ─────────────────────────────────────────────────────────

EXTRACTION_SYSTEM = """\
You are a precise data extraction engine. Your only job is to extract structured
customer onboarding data from the raw text provided and return it as a single
valid JSON object. Do not include any prose, markdown, code fences, or explanation —
output the JSON object only.

Required schema:
{
  "crm_id":    <string or null>,
  "name":      <string>,
  "email":     <string>,
  "company":   <string>,
  "tier":      <"free" | "pro" | "enterprise" | null>,
  "phone":     <string or null>,
  "address":   <string or null>,
  "metadata":  <object — any additional key/value pairs found>
}

Rules:
- If a field is not present in the text, use null (not an empty string).
- Normalise email to lowercase.
- tier must be exactly one of the four allowed values, or null.
- metadata may contain arbitrary additional fields found in the source.
- If the text contains multiple customers, return only the most prominent one.
"""

EXTRACTION_USER = """\
Extract the customer data from the following text:

---
{chunk}
---
"""

SELF_CORRECTION_USER = """\
The JSON below is invalid or malformed. Fix it so it is valid JSON and matches
the required schema. Return only the corrected JSON — no prose, no code fences.

Invalid JSON:
{bad_json}

Parse error:
{error}
"""


# ── Main parser ───────────────────────────────────────────────────────────────


class LLMParser:
    def __init__(self, model: str = MODEL) -> None:
        self._client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        self._model = model

    def parse(self, chunk: str) -> dict[str, Any]:
        """
        Parse a raw text chunk into a structured customer dict.

        Raises:
            ValueError: if JSON cannot be recovered after MAX_SELF_CORRECTIONS rounds.
        """
        raw = self._call_claude(
            system=EXTRACTION_SYSTEM,
            user=EXTRACTION_USER.format(chunk=chunk),
        )
        return self._parse_with_correction(raw, original_chunk=chunk)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _call_claude(self, *, system: str, user: str) -> str:
        response = self._client.messages.create(
            model=self._model,
            max_tokens=MAX_TOKENS,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return response.content[0].text.strip()

    def _parse_with_correction(
        self, raw: str, original_chunk: str, attempt: int = 0
    ) -> dict[str, Any]:
        """Recursively attempt to parse JSON, self-correcting up to MAX_SELF_CORRECTIONS."""
        try:
            # Strip accidental code fences the model sometimes adds
            cleaned = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            result: dict[str, Any] = json.loads(cleaned)
            if not isinstance(result, dict):
                raise ValueError(f"Expected JSON object, got {type(result).__name__}")
            log.info("llm_parse_success", attempt=attempt)
            return result
        except (json.JSONDecodeError, ValueError) as exc:
            if attempt >= MAX_SELF_CORRECTIONS:
                log.error(
                    "llm_parse_failed_permanently",
                    attempt=attempt,
                    error=str(exc),
                    raw_preview=raw[:200],
                )
                raise ValueError(
                    f"LLM output could not be parsed as JSON after "
                    f"{MAX_SELF_CORRECTIONS} self-correction attempts: {exc}"
                ) from exc

            log.warning(
                "llm_parse_retrying_self_correction",
                attempt=attempt,
                error=str(exc),
            )
            corrected = self._call_claude(
                system=EXTRACTION_SYSTEM,
                user=SELF_CORRECTION_USER.format(bad_json=raw, error=str(exc)),
            )
            return self._parse_with_correction(corrected, original_chunk, attempt + 1)
