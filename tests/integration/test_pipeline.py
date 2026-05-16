"""
tests/integration/test_pipeline.py
End-to-end pipeline test using moto (mocked AWS) and a stub LLM parser.

Run with:
    pytest tests/integration/ -v
"""

from __future__ import annotations

import json
import os
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest

# ── Environment setup (must happen before importing src) ──────────────────────
os.environ.pop("AWS_ENDPOINT_URL", None)   # let moto intercept cleanly
os.environ.update(
    {
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "S3_RAW_BUCKET": "test-raw",
        "S3_FAILED_BUCKET": "test-failed",
        "DYNAMODB_TABLE": "test-idempotency",
        "SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123/test-queue",
        "CRM_BASE_URL": "https://crm.test/api/v1",
        "CRM_API_KEY": "test-key",
        "ANTHROPIC_API_KEY": "test-key",
        "CRM_RPS_LIMIT": "100",
        "MAX_RETRIES": "1",
        "LOG_LEVEL": "WARNING",
    }
)

from moto import mock_aws  # noqa: E402

from src.agents.ingestor import Ingestor  # noqa: E402
from src.agents.orchestrator import Orchestrator  # noqa: E402
from src.dlq_handler import DLQHandler  # noqa: E402
from src.monitoring.cloudwatch import CloudWatchMetrics  # noqa: E402
from src.validators.schema_validator import SchemaValidator  # noqa: E402


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def aws_setup():
    """Create mocked AWS resources for each test."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-raw")
        s3.create_bucket(Bucket="test-failed")

        dynamo = boto3.resource("dynamodb", region_name="us-east-1")
        dynamo.create_table(
            TableName="test-idempotency",
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        cw = boto3.client("cloudwatch", region_name="us-east-1")
        yield {"s3": s3, "dynamo": dynamo, "cw": cw}


def _good_record() -> dict[str, Any]:
    return {
        "crm_id": None,
        "name": "Bob Jones",
        "email": "bob@contoso.com",
        "company": "Contoso",
        "tier": "enterprise",
        "phone": None,
        "address": None,
        "metadata": {},
    }


def _make_orchestrator(mock_parse_return: dict | None = None) -> Orchestrator:
    mock_parser = MagicMock()
    mock_parser.parse.return_value = mock_parse_return or _good_record()

    mock_crm = MagicMock()
    mock_crm.upsert.return_value = ("created", {"id": "crm-001"})

    return Orchestrator(
        parser=mock_parser,
        validator=SchemaValidator(),
        crm_client=mock_crm,
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_happy_path_single_record(aws_setup) -> None:
    s3 = aws_setup["s3"]
    content = b"Name: Bob Jones\nEmail: bob@contoso.com\nCompany: Contoso\nTier: enterprise"
    s3.put_object(Bucket="test-raw", Key="2026/bob.txt", Body=content)

    orch = _make_orchestrator()
    result = orch.process_s3_event("test-raw", "2026/bob.txt")

    assert result.success == 1
    assert result.failed == 0


def test_invalid_record_goes_to_dlq(aws_setup) -> None:
    s3 = aws_setup["s3"]
    s3.put_object(Bucket="test-raw", Key="2026/bad.txt", Body=b"garbage data")

    bad_record = {"crm_id": None, "name": "X", "email": "not-an-email", "company": "Y",
                  "tier": None, "phone": None, "address": None, "metadata": None}
    orch = _make_orchestrator(mock_parse_return=bad_record)
    result = orch.process_s3_event("test-raw", "2026/bad.txt")

    assert result.failed == 1
    assert result.success == 0

    # Verify something was written to the DLQ bucket
    resp = s3.list_objects_v2(Bucket="test-failed")
    assert resp.get("KeyCount", 0) >= 1


def test_idempotency_skips_already_processed(aws_setup) -> None:
    s3 = aws_setup["s3"]
    s3.put_object(Bucket="test-raw", Key="2026/dup.txt", Body=b"duplicate file")

    orch = _make_orchestrator()

    result1 = orch.process_s3_event("test-raw", "2026/dup.txt")
    result2 = orch.process_s3_event("test-raw", "2026/dup.txt")

    assert result1.success == 1
    assert result2.total == 0           # skipped — no records processed
    # Parser should only have been called once
    orch._parser.parse.assert_called_once()


def test_crm_failure_routes_to_dlq(aws_setup) -> None:
    s3 = aws_setup["s3"]
    s3.put_object(Bucket="test-raw", Key="2026/crm_fail.txt", Body=b"valid data")

    mock_parser = MagicMock()
    mock_parser.parse.return_value = _good_record()

    mock_crm = MagicMock()
    mock_crm.upsert.side_effect = RuntimeError("CRM exploded")

    orch = Orchestrator(
        parser=mock_parser,
        validator=SchemaValidator(),
        crm_client=mock_crm,
    )

    result = orch.process_s3_event("test-raw", "2026/crm_fail.txt")
    assert result.failed == 1

    resp = s3.list_objects_v2(Bucket="test-failed")
    assert resp.get("KeyCount", 0) >= 1
