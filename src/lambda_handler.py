"""
src/lambda_handler.py
AWS Lambda entrypoint.

Triggered by SQS events from the S3 bucket notification.
Each SQS message contains one or more S3 event records.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

from dotenv import load_dotenv

from src.agents.orchestrator import Orchestrator
from src.monitoring.logger import bind_correlation_id, clear_context, configure_logging, get_logger

load_dotenv()  # no-op in Lambda (env vars already set); useful for local runs

configure_logging(level=os.environ.get("LOG_LEVEL", "INFO"))
log = get_logger("lambda_handler")

# Instantiate once per cold start — survives across warm invocations
_orchestrator = Orchestrator()


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Lambda handler for SQS → S3 events.

    Expected event shape (SQS wrapping S3 notification):
    {
      "Records": [
        {
          "body": "{\"Records\": [{\"s3\": {\"bucket\": {\"name\": \"...\"},
                                           \"object\": {\"key\": \"...\"}}}]}"
        }
      ]
    }
    """
    clear_context()
    correlation_id = getattr(context, "aws_request_id", str(uuid.uuid4()))
    bind_correlation_id(correlation_id)

    sqs_records: list[dict] = event.get("Records", [])
    log.info("lambda_invoked", sqs_record_count=len(sqs_records))

    all_results = []
    batch_item_failures: list[dict] = []

    for sqs_record in sqs_records:
        message_id = sqs_record.get("messageId", "unknown")
        try:
            s3_event = _parse_sqs_body(sqs_record)
            for s3_record in s3_event.get("Records", []):
                bucket = s3_record["s3"]["bucket"]["name"]
                key    = s3_record["s3"]["object"]["key"]
                result = _orchestrator.process_s3_event(bucket, key)
                all_results.append(result)
        except Exception as exc:
            log.error(
                "sqs_record_processing_failed",
                message_id=message_id,
                error=str(exc),
            )
            # Report to SQS so the message is retried / sent to DLQ
            batch_item_failures.append({"itemIdentifier": message_id})

    total_success = sum(r.success for r in all_results)
    total_failed  = sum(r.failed  for r in all_results)

    log.info(
        "lambda_complete",
        total_success=total_success,
        total_failed=total_failed,
        sqs_failures=len(batch_item_failures),
    )

    # Partial batch response — only failed SQS messages are retried
    return {"batchItemFailures": batch_item_failures}


def _parse_sqs_body(record: dict[str, Any]) -> dict[str, Any]:
    body = record.get("body", "{}")
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid SQS message body: {exc}") from exc
