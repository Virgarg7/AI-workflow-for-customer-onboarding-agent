# 🤖 Enterprise AI-Driven Customer Onboarding Agent

> Automate customer onboarding end-to-end: ingest unstructured data from AWS S3, parse it with Claude (LLM), validate it, and write to a legacy CRM — with full production resilience.

---

## 📌 Problem Statement

Enterprise clients have customer data sitting in AWS S3 as unstructured files (PDFs, CSVs, emails, contracts). Their legacy CRM has an undocumented REST API that rate-limits and fails unpredictably. Manual onboarding took **4–6 days per customer** with an **18% error rate**.

This agent reduces that to **fully automated, sub-hour processing** with zero data loss.

---

## 🏗️ Architecture

```
S3 (raw files)
  └─► SQS Queue
        └─► Ingestor Lambda          ← chunking + DynamoDB idempotency
              └─► LLM Parser         ← Claude Sonnet 4 structured extraction
                    └─► Validator    ← JSON Schema + business rules
                          ├─► Orchestrator ──► Legacy CRM API
                          │         └─► Token Bucket + Circuit Breaker + Retry
                          └─► Dead Letter Queue ──► S3 /failed/
                                        └─► CloudWatch + PagerDuty
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| SQS buffer between S3 and Lambda | Decouples ingestion; free retry buffer; no data loss on Lambda failure |
| DynamoDB idempotency key | Prevents duplicate CRM writes from SQS at-least-once delivery |
| LLM self-correction (max 2×) | Recovers from OCR noise without infinite loops |
| Token bucket before circuit breaker before retry | Each layer solves a different failure mode in the right order |
| DLQ → S3 audit trail | Never silently drop records; ops can replay without reprocessing |

---

## ✨ Features

- **Multi-format ingestion** — PDF, CSV, plain text, JSON, email threads from S3
- **LLM-powered parsing** — Claude Sonnet 4 extracts structured `{name, email, company, tier, metadata}` from any format
- **Self-correction loop** — LLM auto-fixes malformed JSON output (max 2 rounds before DLQ)
- **Schema validation** — JSON Schema + Pydantic + business rules (email regex, blocked domains, dedup)
- **3-layer resilience**:
  - 🪣 **Token bucket** — enforces CRM RPS limit before calls go out
  - ⚡ **Circuit breaker** — trips at 50% error rate; auto-recovers after 60s
  - 🔁 **Exponential backoff** — 1→2→4→8→16s, max 5 retries; honours `Retry-After` on 429
- **Dead Letter Queue** — failures written to S3 `/failed/` with full error context
- **Observability** — CloudWatch custom metrics, PagerDuty alerting, structured JSON logs
- **CI/CD** — GitHub Actions: lint + unit + integration tests on PR; deploy on merge to main

---

## 📁 Project Structure

```
enterprise-onboarding-agent/
├── src/
│   ├── lambda_handler.py          # AWS Lambda entrypoint (SQS events)
│   ├── crm_client.py              # Legacy CRM REST adapter
│   ├── dlq_handler.py             # Dead Letter Queue writer + PagerDuty alerts
│   ├── agents/
│   │   ├── ingestor.py            # S3 download, chunking, DynamoDB idempotency
│   │   └── orchestrator.py        # Central pipeline coordinator
│   ├── parsers/
│   │   └── llm_parser.py          # Claude Sonnet 4 extraction + self-correction
│   ├── validators/
│   │   └── schema_validator.py    # JSON Schema + Pydantic + business rules
│   ├── resilience/
│   │   ├── token_bucket.py        # Thread-safe token bucket rate limiter
│   │   ├── circuit_breaker.py     # 3-state circuit breaker
│   │   └── retry_handler.py       # Exponential backoff via tenacity
│   └── monitoring/
│       ├── logger.py              # Structured JSON logger (structlog)
│       └── cloudwatch.py          # Custom CloudWatch metrics emitter
├── tests/
│   ├── unit/                      # 14 pure unit tests (no AWS)
│   └── integration/               # 4 end-to-end tests (moto, fully offline)
├── infra/
│   └── docker/docker-compose.yml  # Localstack for local development
├── scripts/
│   ├── seed_s3.py                 # Seeds local Localstack with sample files
│   ├── deploy.sh                  # Build zip → update Lambda → smoke test
│   └── test_event.json            # Local Lambda invocation payload
├── docs/
│   └── architecture.md            # Architecture Decision Records (ADRs)
├── .github/workflows/
│   ├── ci.yml                     # Test + lint on every PR
│   └── deploy.yml                 # Deploy to Lambda on merge to main
├── .env.example
├── Makefile
└── pyproject.toml
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- AWS CLI configured
- Docker (for local dev with Localstack)
- An [Anthropic API key](https://console.anthropic.com/)

### Install

```bash
git clone https://github.com/<your-username>/enterprise-onboarding-agent.git
cd enterprise-onboarding-agent

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements-dev.txt
cp .env.example .env
# Edit .env — add your ANTHROPIC_API_KEY and CRM_BASE_URL
```

### Run locally (Localstack)

```bash
make local-up      # Start Localstack + create S3/SQS/DynamoDB resources
make seed          # Upload sample customer files to local S3
make run-local     # Invoke the Lambda handler locally
make logs          # Tail structured JSON logs
```

### Run tests

```bash
make test-unit         # 14 unit tests — fast, no AWS
make test-integration  # 4 integration tests — moto mocked AWS, fully offline
make test              # All 23 tests
make lint              # Ruff + mypy
```

All tests run offline — no AWS account or API key required.

### Deploy to AWS

```bash
make deploy ENV=staging
make deploy ENV=production
```

---

## ⚙️ Configuration

All configuration via environment variables (`.env.example` provided):

| Variable | Description | Default |
|---|---|---|
| `ANTHROPIC_API_KEY` | Claude API key | **required** |
| `CRM_BASE_URL` | Legacy CRM REST base URL | **required** |
| `CRM_API_KEY` | CRM authentication key | **required** |
| `S3_RAW_BUCKET` | Input S3 bucket | **required** |
| `S3_FAILED_BUCKET` | DLQ output S3 bucket | **required** |
| `DYNAMODB_TABLE` | Idempotency table name | **required** |
| `SQS_QUEUE_URL` | Input SQS queue URL | **required** |
| `CRM_RPS_LIMIT` | Max CRM requests/second | `5` |
| `MAX_RETRIES` | CRM write retry attempts | `5` |
| `CB_ERROR_THRESHOLD` | Circuit breaker trip ratio | `0.5` |
| `CB_WINDOW_SECONDS` | Circuit breaker window | `60` |
| `CLOUDWATCH_NAMESPACE` | Metrics namespace | `OnboardingAgent` |
| `PAGERDUTY_KEY` | PagerDuty routing key | optional |

---

## 🛡️ Resilience — Failure Mode Matrix

| Failure | Detection | Mitigation | Outcome |
|---|---|---|---|
| CRM 429 rate limit | HTTP 429 + `Retry-After` | Honour header delay + token bucket prevents future bursts | Record retried within SLA |
| CRM 5xx transient | HTTP 500/502/503/504 | Retry up to 5× with exponential backoff (1→16s) | Resolved on retry |
| CRM sustained outage | Error rate > 50% over 60s | Circuit breaker OPENS — fast-fail; probe after 60s | Auto-recovers when CRM stabilises |
| LLM malformed JSON | `json.JSONDecodeError` | Self-correction loop (max 2 rounds) → DLQ | Most OCR noise healed; bad records isolated |
| Corrupt S3 file | Decode error / empty chunks | Log + DLQ with `s3_key` + error detail | Pipeline continues; ops alerted |
| DLQ depth spike | CloudWatch alarm | PagerDuty + Slack alert | Human triage before flood worsens |
| Duplicate S3 event | DynamoDB idempotency check | Duplicate silently skipped | Exactly-once CRM write guaranteed |

---

## 🔄 CI/CD

GitHub Actions workflows run automatically:

| Trigger | Workflow | Steps |
|---|---|---|
| Pull Request | `ci.yml` | Ruff lint → mypy type-check → unit tests → integration tests |
| Merge to `main` | `deploy.yml` | Build Lambda zip → deploy to staging → smoke test |
| Manual dispatch | `deploy.yml` | Choose `staging` or `production` |

### Required GitHub Secrets

Set these under **Settings → Secrets and variables → Actions**:

```
ANTHROPIC_API_KEY       ← Your Claude API key
CRM_BASE_URL            ← https://your-legacy-crm.com/api/v1
CRM_API_KEY             ← CRM authentication key
AWS_DEPLOY_ROLE_ARN     ← IAM role ARN for OIDC deployment
AWS_REGION              ← e.g. us-east-1
```

---

## 📊 Observability

| Signal | Tool | Metric / Event |
|---|---|---|
| Throughput | CloudWatch | `RecordsProcessed`, `RecordsFailed` per batch |
| Error rate | CloudWatch | `ErrorRate` (0.0–1.0) per batch |
| Circuit breaker | CloudWatch | `CircuitBreakerOpen` count |
| Retry depth | CloudWatch | `RetryAttempt` by attempt number |
| DLQ spike | PagerDuty | Alert when DLQ depth > 50 per invocation |
| All events | structlog | JSON logs to CloudWatch Logs with `correlation_id` |

---

## 🗂️ Architecture Decision Records

See [`docs/architecture.md`](docs/architecture.md) for the full ADR log covering:
- ADR-001: SQS between S3 and Lambda
- ADR-002: DynamoDB idempotency over SQS FIFO deduplication
- ADR-003: Token bucket → circuit breaker → retry ordering
- ADR-004: LLM self-correction capped at 2 rounds
- ADR-005: No bulk CRM API

---

## 📄 License

MIT — see [LICENSE](LICENSE)
