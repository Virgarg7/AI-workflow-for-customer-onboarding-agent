"""
src/agents/orchestrator.py
Central pipeline coordinator.

For each S3 event:
  1. Ingest + chunk the S3 object
  2. Parse each chunk with the LLM
  3. Validate the parsed record
  4. Write to CRM through resilience layer (token bucket + circuit breaker + retry)
  5. Route failures to DLQ
  6. Emit CloudWatch metrics
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from src.agents.ingestor import Chunk, Ingestor
from src.crm_client import CRMClient
from src.dlq_handler import DLQHandler
from src.monitoring.cloudwatch import CloudWatchMetrics
from src.monitoring.logger import get_logger
from src.parsers.llm_parser import LLMParser
from src.resilience.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from src.resilience.retry_handler import with_retry
from src.resilience.token_bucket import TokenBucket
from src.validators.schema_validator import SchemaValidator

log = get_logger("orchestrator")

CRM_RPS_LIMIT      = float(os.environ.get("CRM_RPS_LIMIT", "5"))
MAX_RETRIES        = int(os.environ.get("MAX_RETRIES", "5"))
CB_ERROR_THRESHOLD = float(os.environ.get("CB_ERROR_THRESHOLD", "0.5"))
CB_WINDOW_SECONDS  = float(os.environ.get("CB_WINDOW_SECONDS", "60"))


@dataclass
class PipelineResult:
    s3_key: str
    success: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.success + self.failed

    @property
    def error_rate(self) -> float:
        return self.failed / self.total if self.total else 0.0


class Orchestrator:
    def __init__(
        self,
        ingestor: Ingestor | None = None,
        parser: LLMParser | None = None,
        validator: SchemaValidator | None = None,
        crm_client: CRMClient | None = None,
        dlq: DLQHandler | None = None,
        metrics: CloudWatchMetrics | None = None,
    ) -> None:
        self._ingestor  = ingestor  or Ingestor()
        self._parser    = parser    or LLMParser()
        self._validator = validator or SchemaValidator()
        self._crm       = crm_client or CRMClient()
        self._dlq       = dlq       or DLQHandler()
        self._metrics   = metrics   or CloudWatchMetrics()

        # Shared resilience primitives (one per Orchestrator instance → Lambda container)
        self._bucket = TokenBucket(rate=CRM_RPS_LIMIT)
        self._breaker = CircuitBreaker(
            name="legacy-crm",
            error_threshold=CB_ERROR_THRESHOLD,
            window_seconds=CB_WINDOW_SECONDS,
        )

    # ── Main entry point ──────────────────────────────────────────────────────

    def process_s3_event(self, bucket: str, key: str) -> PipelineResult:
        result = PipelineResult(s3_key=key)

        ingest_result = self._ingestor.ingest(bucket, key)
        if ingest_result.skipped:
            log.info("pipeline_skipped_already_processed", key=key)
            return result

        for chunk in ingest_result.chunks:
            self._process_chunk(chunk, result)

        self._ingestor.mark_complete(key, result.success, result.failed)
        self._metrics.record_batch(
            success=result.success,
            failed=result.failed,
            source_key=key,
        )
        log.info(
            "pipeline_complete",
            key=key,
            success=result.success,
            failed=result.failed,
            error_rate=round(result.error_rate, 3),
        )
        return result

    # ── Per-chunk processing ──────────────────────────────────────────────────

    def _process_chunk(self, chunk: Chunk, result: PipelineResult) -> None:
        # ── Step 1: LLM Parse ─────────────────────────────────────────────────
        try:
            raw_record = self._parser.parse(chunk.text)
        except Exception as exc:
            log.warning("parse_failed", chunk_id=chunk.chunk_id, error=str(exc))
            self._dlq.write(
                source_key=chunk.s3_key,
                chunk_index=chunk.index,
                error=str(exc),
                reason_code="llm_parse_error",
            )
            result.failed += 1
            result.errors.append(f"chunk#{chunk.index}: parse_error")
            return

        # ── Step 2: Validate ──────────────────────────────────────────────────
        validation = self._validator.validate(raw_record)
        if not validation:
            log.warning(
                "validation_failed",
                chunk_id=chunk.chunk_id,
                reason=validation.reason,
            )
            self._dlq.write(
                source_key=chunk.s3_key,
                chunk_index=chunk.index,
                record=raw_record,
                error=validation.reason,
                reason_code="validation_failed",
            )
            result.failed += 1
            result.errors.append(f"chunk#{chunk.index}: {validation.reason}")
            return

        assert validation.record is not None  # guaranteed when valid=True
        crm_payload = validation.record.model_dump(exclude_none=False)

        # ── Step 3: Resilient CRM write ───────────────────────────────────────
        try:
            self._write_to_crm(crm_payload, chunk)
            result.success += 1
        except CircuitBreakerOpenError as exc:
            log.error("circuit_open_aborting_chunk", chunk_id=chunk.chunk_id)
            self._metrics.record_circuit_breaker_open("legacy-crm")
            self._dlq.write(
                source_key=chunk.s3_key,
                chunk_index=chunk.index,
                record=crm_payload,
                error=str(exc),
                reason_code="circuit_breaker_open",
            )
            result.failed += 1
            result.errors.append(f"chunk#{chunk.index}: circuit_open")
        except Exception as exc:
            log.error("crm_write_exhausted_retries", chunk_id=chunk.chunk_id, error=str(exc))
            self._dlq.write(
                source_key=chunk.s3_key,
                chunk_index=chunk.index,
                record=crm_payload,
                error=str(exc),
                reason_code="crm_write_failed",
            )
            result.failed += 1
            result.errors.append(f"chunk#{chunk.index}: crm_write_failed")

    def _write_to_crm(self, payload: dict[str, Any], chunk: Chunk) -> None:
        """Token-bucket gate → circuit breaker → retry → actual HTTP call."""

        def _do_write() -> None:
            self._bucket.consume()  # blocks until rate-limit token available
            action, _ = self._breaker.call(lambda: self._crm.upsert(payload))
            log.info(
                "crm_write_success",
                action=action,
                email=payload.get("email"),
                chunk_id=chunk.chunk_id,
            )

        with_retry(_do_write, max_attempts=MAX_RETRIES)
