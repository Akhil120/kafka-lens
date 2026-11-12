# KafkaLens

A fast, locally-hosted Kafka message browser built as a drop-in replacement for the Confluent Control Center UI, which crashes under Chrome memory pressure on large topics.

**Stack:** FastAPI · DuckDB · confluent-kafka (librdkafka) · Alpine.js · Tailwind CSS — no JavaScript build step.

---

## Quick start

```bash
# 1. Start Kafka + Schema Registry
docker compose up -d

# 2. Start the app (creates .venv automatically on first run)
./start.sh
# → http://localhost:8000

# 3. Add your cluster in the UI and start searching
```

**Default local cluster credentials** (set in `config/kafka-jaas.conf`):
- Bootstrap: `localhost:9092` · Protocol: `SASL_PLAINTEXT` · Mechanism: `PLAIN`
- Username: `kafkalens` / Password: `kafkalens-secret`
- Schema Registry: `http://localhost:8081`

---

## Features

- **Real-time SSE streaming** — progress bar shows messages scanned vs. matched while the search runs; Cancel aborts immediately
- **Avro / JSON / Protobuf / plain-text auto-detection** — magic byte + schema ID decoded against Confluent Schema Registry
- **DuckDB SQL engine** — every batch of messages is inserted into an in-process columnar table; filters run as real SQL
- **Paginated results** — rows-per-page selector (25 / 50 / 100 / 200), jump-to-page input, Prev / Next — no endless scroll
- **Fernet-encrypted credential storage** — passwords and SR secrets never stored in plain text (`~/.kafka-lens/`)
- **Dark / light theme** — persisted in `localStorage`

---

## Search filters

| Mode | Input example | How it works |
|---|---|---|
| **Full-text** | `checkout_complete` | `ILIKE '%...%'` on the raw string value |
| **JSONPath** | `$.geo.country=DE` | `json_extract_string(value_json, '$.geo.country') = 'DE'` |
| **SQL** | `json_extract_string(value_json,'$.geo.country')='DE' AND CAST(json_extract(value_json,'$.user.lifetime_value') AS DOUBLE) > 10000` | Passed directly as a DuckDB `WHERE` clause fragment; `value_json` and `key` are the available columns |

**Key filter** supports `contains` / `prefix` / `exact` / `regex` match modes.

**Scope controls:** from/to timestamp, from/to offset, partition list, format override, message limit.

---

## Supported authentication types

| Security Protocol | SASL Mechanism | Notes |
|---|---|---|
| `PLAINTEXT` | — | No auth |
| `SASL_PLAINTEXT` | `PLAIN` | Username + password |
| `SASL_PLAINTEXT` | `SCRAM-SHA-256` / `SCRAM-SHA-512` | Username + password |
| `SASL_PLAINTEXT` | `OAUTHBEARER` | Token callback |
| `SSL` | — | mTLS; CA / cert / key file paths |
| `SASL_SSL` | `PLAIN` | Confluent Cloud (API key + secret) |
| `SASL_SSL` | `SCRAM-SHA-256` / `SCRAM-SHA-512` | SCRAM over TLS |
| `SASL_SSL` | `OAUTHBEARER` | OIDC / OAuth over TLS |

---

## Architecture

```
Browser (Tailwind + Alpine.js)
        │  HTTP + SSE
FastAPI (Python 3.11+)
  ├── Cluster store   ── Fernet-encrypted JSON  (~/.kafka-lens/clusters.json)
  ├── Kafka module    ── confluent-kafka / librdkafka C lib
  ├── Schema module   ── fastavro / Confluent SR HTTP client
  └── Search engine   ── DuckDB in-process (vectorized SQL on message batches)
        │
Kafka (KRaft, Docker) + Confluent Schema Registry (Docker)
```

**Why these choices:**
- `confluent-kafka` over `kafka-python`: C-based librdkafka is 10–100× faster for high-throughput consumption
- **DuckDB**: in-process columnar DB; native JSON functions, no external service
- **SSE** over WebSockets: unidirectional streaming (server → client) with automatic reconnect; simpler than WS for this use case
- **Jinja2 + Alpine.js**: full Python stack, no build step, SPA feel with zero bundler complexity

### Search flow (per-request)

1. `POST /api/clusters/{id}/topics/{topic}/search` → returns `{task_id}`
2. `GET /api/search/{task_id}/stream` opens SSE
3. AdminClient resolves timestamp range → per-partition start offsets
4. Consumer seeks each partition; consumes in batches of 500 messages
5. Each batch → deserializer (Avro / JSON / Protobuf / plain) → Python dicts
6. Batch inserted into DuckDB in-memory table (`value_json JSON`, `key VARCHAR`, `msg_offset BIGINT`, ...)
7. SQL filter applied; matching rows appended to `results`
8. `event: progress` sent every batch; `event: done` closes stream with final payload
9. `DELETE /api/search/{task_id}` cancels an in-flight task

---

## File structure

```
kafka-lens/
├── docker-compose.yml          Kafka (KRaft) + Schema Registry
├── requirements.txt
├── run.py                      uvicorn entry point
├── start.sh                    One-command bootstrap (creates .venv)
├── config/
│   └── kafka-jaas.conf         SASL credentials for the Docker broker
├── app/
│   ├── main.py                 FastAPI app, static/template mounts
│   ├── config.py               Pydantic settings
│   ├── models.py               All Pydantic request/response models
│   ├── crypto.py               Fernet key generation + encrypt/decrypt
│   ├── store.py                Cluster config persistence
│   ├── kafka/
│   │   ├── auth.py             librdkafka config dict for all auth types
│   │   └── consumer.py         AdminClient + batch message fetcher
│   ├── schema/
│   │   ├── registry.py         Async HTTP client for Confluent Schema Registry
│   │   └── deserializer.py     Auto-detect + deserialize Avro/JSON/Protobuf/plain
│   ├── search/
│   │   └── engine.py           DuckDB table, SQL filter builder, SSE emit
│   └── api/
│       ├── clusters.py         /api/clusters CRUD + /test endpoint
│       ├── topics.py           /api/topics list + metadata
│       └── search.py           POST search → task_id; GET SSE stream; DELETE cancel
├── templates/
│   └── index.html              Full UI (sidebar + search form + results table)
├── static/
│   ├── css/app.css             Custom vars, scrollbar, animations, format badges
│   └── js/app.js               Alpine.js app: SSE handler, pagination, cluster CRUD
└── tests/
    └── test_ui.py              35 end-to-end tests (Playwright + httpx)
```

---

## API reference

```
Cluster management
  GET    /api/clusters                              List clusters
  POST   /api/clusters                              Create cluster (encrypts secrets)
  PUT    /api/clusters/{id}                         Update cluster
  DELETE /api/clusters/{id}                         Remove cluster
  POST   /api/clusters/{id}/test                    Test connection → broker list

Topics
  GET    /api/clusters/{id}/topics                  List topics (name, partitions, offsets)
  GET    /api/clusters/{id}/topics/{topic}/info     Partitions, watermarks, schema subject

Search
  POST   /api/clusters/{id}/topics/{topic}/search   Start search → {task_id}
  GET    /api/search/{task_id}/stream               SSE: progress / results / done / error
  DELETE /api/search/{task_id}                      Cancel in-flight search
```

### SSE event types

| Event | Payload |
|---|---|
| `progress` | `{"scanned": N, "matched": M}` — emitted every batch |
| `results` | `[{partition, offset, timestamp_human, key, value, value_raw, format, headers}]` — intermediate results |
| `done` | Same as `results` plus final `scanned` / `matched` counts |
| `error` | `{"error": "..."}` |

---

## Credential storage

- Key file: `~/.kafka-lens/key` — Fernet 32-byte key, generated once on first run, never leaves the machine
- Config file: `~/.kafka-lens/clusters.json` — encrypted fields: `password`, `schema_registry_password`, `ssl_key_password`
- Cluster `id` is a UUID; display name is user-defined

---

## Running tests

```bash
# Requires: app on :8000, Kafka on :9092, Schema Registry on :8081
# Topic "analytics.events" must exist and contain messages

.venv/bin/pip install playwright pytest-playwright httpx
.venv/bin/playwright install chromium

.venv/bin/pytest tests/test_ui.py -v
# → 35 passed
```

Tests cover: API validation (422s, 404s), page load, Add Cluster modal field visibility, topic filter, all search modes (no-filter, JSONPath, full-text, key prefix, SQL compound), cancel mid-search, row expand, JSON export, and schema deserialisation correctness.
