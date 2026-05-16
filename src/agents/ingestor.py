"""
src/agents/ingestor.py
Download S3 objects, detect MIME type, chunk into LLM-safe windows,
and track progress in DynamoDB for exactly-once delivery.
"""

from __future__ import annotations

import hashlib
import io
import os
from dataclasses import dataclass, field
from typing import Iterator

import boto3
from botocore.exceptions import ClientError

from src.monitoring.logger import get_logger

log = get_logger("ingestor")

MAX_CHUNK_CHARS = 12_000   # ~4K tokens at ~3 chars/token
OVERLAP_CHARS   = 500       # sliding window overlap to avoid splitting mid-record

DYNAMODB_TABLE  = os.environ.get("DYNAMODB_TABLE", "onboarding-idempotency")
S3_RAW_BUCKET   = os.environ.get("S3_RAW_BUCKET", "corp-onboarding-raw")
S3_PROCESSED_PREFIX = "processed/"


@dataclass
class Chunk:
    chunk_id: str
    s3_key: str
    index: int
    text: str
    total_chunks: int


@dataclass
class IngestResult:
    s3_key: str
    chunks: list[Chunk] = field(default_factory=list)
    skipped: bool = False   # True if already processed (idempotency)


class Ingestor:
    def __init__(self) -> None:
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        region = os.environ.get("AWS_REGION", "us-east-1")
        self._s3 = boto3.client("s3", endpoint_url=endpoint, region_name=region)
        self._dynamo = boto3.resource("dynamodb", endpoint_url=endpoint, region_name=region)
        self._table = self._dynamo.Table(DYNAMODB_TABLE)

    # ── Public API ────────────────────────────────────────────────────────────

    def ingest(self, bucket: str, key: str) -> IngestResult:
        """
        Download the S3 object at bucket/key, split into chunks.
        Returns IngestResult.skipped=True if this key was already processed.
        """
        if self._already_processed(key):
            log.info("ingest_skipped_duplicate", key=key)
            return IngestResult(s3_key=key, skipped=True)

        log.info("ingest_start", bucket=bucket, key=key)
        text = self._download(bucket, key)
        chunks = list(self._chunk(text, key))
        log.info("ingest_chunked", key=key, num_chunks=len(chunks))
        return IngestResult(s3_key=key, chunks=chunks)

    def mark_complete(self, key: str, success: int, failed: int) -> None:
        """Mark the S3 key as processed in DynamoDB and move it in S3."""
        try:
            self._table.put_item(
                Item={
                    "pk": f"s3key#{key}",
                    "sk": "status",
                    "status": "complete",
                    "success": success,
                    "failed": failed,
                }
            )
        except ClientError as exc:
            log.warning("dynamo_write_failed", key=key, error=str(exc))

    # ── Download ──────────────────────────────────────────────────────────────

    def _download(self, bucket: str, key: str) -> str:
        try:
            obj = self._s3.get_object(Bucket=bucket, Key=key)
            raw_bytes: bytes = obj["Body"].read()
        except ClientError as exc:
            raise RuntimeError(f"S3 download failed for s3://{bucket}/{key}: {exc}") from exc

        content_type: str = obj.get("ContentType", "")
        return self._decode(raw_bytes, content_type, key)

    def _decode(self, data: bytes, content_type: str, key: str) -> str:
        """Best-effort decoding; handles UTF-8, latin-1, and binary fallback."""
        # PDF: extract text with pdfminer if available, else raw decode
        if "pdf" in content_type or key.lower().endswith(".pdf"):
            return self._extract_pdf_text(data)

        for encoding in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                return data.decode(encoding)
            except (UnicodeDecodeError, LookupError):
                continue

        log.warning("decode_fallback_to_repr", key=key)
        return data.decode("utf-8", errors="replace")

    @staticmethod
    def _extract_pdf_text(data: bytes) -> str:
        """Extract text from PDF bytes; gracefully degrades to raw bytes repr."""
        try:
            from pdfminer.high_level import extract_text  # type: ignore[import]
            return extract_text(io.BytesIO(data))
        except ImportError:
            log.warning("pdfminer_not_installed_falling_back")
            return data.decode("utf-8", errors="replace")
        except Exception as exc:
            log.warning("pdf_extraction_failed", error=str(exc))
            return data.decode("utf-8", errors="replace")

    # ── Chunking ──────────────────────────────────────────────────────────────

    def _chunk(self, text: str, key: str) -> Iterator[Chunk]:
        """
        Sliding-window chunker.  Chunks overlap by OVERLAP_CHARS to avoid
        splitting a customer record across two chunks.
        """
        text = text.strip()
        if not text:
            return

        # Single chunk — fast path
        if len(text) <= MAX_CHUNK_CHARS:
            yield self._make_chunk(key, 0, text, 1)
            return

        positions: list[int] = []
        pos = 0
        while pos < len(text):
            positions.append(pos)
            pos += MAX_CHUNK_CHARS - OVERLAP_CHARS

        total = len(positions)
        for idx, start in enumerate(positions):
            end = start + MAX_CHUNK_CHARS
            yield self._make_chunk(key, idx, text[start:end], total)

    @staticmethod
    def _make_chunk(key: str, index: int, text: str, total: int) -> Chunk:
        chunk_id = hashlib.sha256(f"{key}#{index}".encode()).hexdigest()[:16]
        return Chunk(
            chunk_id=chunk_id,
            s3_key=key,
            index=index,
            text=text,
            total_chunks=total,
        )

    # ── Idempotency ───────────────────────────────────────────────────────────

    def _already_processed(self, key: str) -> bool:
        try:
            resp = self._table.get_item(Key={"pk": f"s3key#{key}", "sk": "status"})
            return resp.get("Item", {}).get("status") == "complete"
        except ClientError as exc:
            log.warning("dynamo_idempotency_check_failed", key=key, error=str(exc))
            return False  # fail open — better to re-process than skip
