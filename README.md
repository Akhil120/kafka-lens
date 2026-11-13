# KafkaLens

A fast, locally-hosted Kafka message browser built as a drop-in replacement for the Confluent Control Center UI, which crashes under Chrome memory pressure on large topics.

**Stack:** FastAPI · DuckDB · confluent-kafka (librdkafka) · Alpine.js · Tailwind CSS — no JavaScript build step.

---

## Quick start

### Option A — pipx install (no Docker, connects to any Kafka)

Requires Python 3.11+ and [pipx](https://pipx.pypa.io).

```bash
pipx install git+https://github.com/Akhil120/kafka-lens.git
kafka-lens
# → http://localhost:8000
```

Upgrades:

```bash
pipx upgrade kafka-lens
```

Cluster configs survive upgrades — stored encrypted in `~/.kafka-lens/`.

> If you don't have pipx: `pip install pipx && pipx ensurepath`, then re-run the install command above.

### Option B — Full Docker stack (Kafka + app together)

Everything in one command:

```bash
docker compose up -d
# → http://localhost:8000
```

Add your first cluster in the UI with these settings:

| Field | Value |
|---|---|
| Bootstrap Servers | `kafka:29092` |
| Security Protocol | `PLAINTEXT` |
| Schema Registry URL | `http://schema-registry:8081` |

> The app container reaches Kafka via the internal PLAINTEXT listener (`kafka:29092`).
> The EXTERNAL SASL listener (`localhost:9092`) is for host-side clients only.

Cluster configs are persisted in a Docker volume (`kafka_lens_data`) and survive restarts.

### Option C — App on host, Kafka in Docker

```bash
# Start Kafka + Schema Registry only
docker compose up -d kafka schema-registry

# Start the app (creates .venv on first run)
./start.sh
# → http://localhost:8000
```

Add your cluster with:

| Field | Value |
|---|---|
| Bootstrap Servers | `localhost:9092` |
| Security Protocol | `SASL_PLAINTEXT` · Mechanism: `PLAIN` |
| Username / Password | `kafkalens` / `kafkalens-secret` |
| Schema Registry URL | `http://localhost:8081` |

---

## Confluent Cloud

KafkaLens connects to Confluent Cloud using `SASL_SSL` + `PLAIN` — the standard API Key auth Confluent uses everywhere. Schema Registry is a separate credential set.

### Step 1 — Create a Cluster API Key

> **Important:** Confluent Cloud has three distinct API key types. You need a **Cluster API key**, not a Cloud API key (for Confluent management APIs) and not a Schema Registry API key.

1. Log in to [confluent.cloud](https://confluent.cloud) and select your **Environment → Cluster**
2. In the left sidebar go to **Data integration → API Keys**
3. Click **+ Add key**
4. Scope: choose **A specific Kafka cluster** (or a service account scoped to it) — do **not** pick "All clusters" or "Cloud resource management"
5. Save the **Key** and **Secret** — the secret is only shown once

### Step 2 — Create a Schema Registry API Key (if you use SR)

Schema Registry lives at the Environment level, not the cluster level.

1. In Confluent Cloud, click your **Environment** name in the top breadcrumb
2. In the right panel, find **Schema Registry** and click **API credentials**
3. Click **+ Add key** and save the **Key** and **Secret**
4. Note the **Schema Registry endpoint URL** shown in that same panel (looks like `https://psrc-xxxxx.us-east-2.aws.confluent.cloud`)

### Step 3 — Add the cluster in KafkaLens

Open **Add Cluster** and fill in:

| Field | Where to find it | Value |
|---|---|---|
| **Bootstrap Servers** | Cluster → Settings → Endpoints → Bootstrap server | `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092` |
| **Security Protocol** | — | `SASL_SSL` |
| **SASL Mechanism** | — | `PLAIN` |
| **Username** | The cluster API Key from Step 1 | `ABCDEFGH1234WXYZ` |
| **Password** | The cluster API Secret from Step 1 | `abc123...` |
| **Schema Registry URL** | Environment → Schema Registry → Settings | `https://psrc-xxxxx.us-east-2.aws.confluent.cloud` |
| **SR Username** | The SR API Key from Step 2 | `SRKEY1234` |
| **SR Password** | The SR API Secret from Step 2 | `srsecret...` |

Click **Test Connection** — you should see `Connected. Brokers: pkc-xxxxx...`.

### Notes

- **Dedicated vs Basic/Standard clusters**: the connection details are identical; only throughput and partition limits differ
- **Private networking (VPC peering / Private Link)**: the bootstrap server address changes but the auth method stays the same — just paste the private endpoint instead
- **Docker stack**: the `kafka-lens` container has outbound internet access by default, so it can reach Confluent Cloud directly
- **Topic list**: internal Confluent topics (`_schemas`, `__consumer_offsets`, etc.) will appear in the topic list — they're safe to ignore

---

## Features

- **Real-time SSE streaming** — progress bar shows messages scanned vs. matched while the search runs; Cancel aborts immediately
- **Avro / JSON / Protobuf / plain-text auto-detection** — magic byte + schema ID decoded against Confluent Schema Registry; Avro-encoded keys also decoded via schema registry
- **DuckDB SQL mode** — messages inserted into an in-process columnar table; filters run as real SQL (`value_json`, `value_raw`, `key` columns available)
- **Partition overview** — per-topic table of low/high watermarks and message count per partition; click **Filter** on any row to scope the search to that partition
- **Resizable sidebar** — drag the divider between sidebar and main panel; width persisted in `localStorage`
- **Autocomplete dropdowns** — topic filter shows matching topics across all loaded clusters as you type; key/value filter fields show recent search history (up to 15 entries per field, stored in `localStorage`)
- **Paginated results** — rows-per-page selector (25 / 50 / 100 / 200), jump-to-page input, Prev / Next — no endless scroll
- **Fernet-encrypted credential storage** — passwords and SR secrets never stored in plain text (`~/.kafka-lens/`)
- **Dark / light theme** — persisted in `localStorage`

---

## Search filters

### Full-text mode (default)

Case-insensitive substring match across the entire raw message value.

```
checkout_complete
error
user-123
```

### JSONPath mode

Match a specific field inside a JSON value. Syntax: `$.path.to.field = expected_value`

```
# Field equals a value
$.status = pending
$.geo.country = DE
$.user.id = 42

# Nested / array access
$.items[0].sku = ABC-999
$.metadata.source = mobile_app

# Field existence check (omit = value)
$.error
$.user.premium
```

- The path uses dot notation; array indices are written as `[0]`, `[1]`, etc.
- The comparison is always a **string equality** check — cast to the right type in SQL mode if you need numeric comparisons.
- If the field is missing the message is excluded.

### SQL mode

The value filter is injected directly into a DuckDB `WHERE` clause. Available columns:

| Column | Type | Description |
|---|---|---|
| `value_raw` | `VARCHAR` | Full message value as a string |
| `value_json` | `JSON` | Parsed JSON (use `json_extract` / `json_extract_string`) |
| `key` | `VARCHAR` | Deserialized message key |
| `partition` | `INTEGER` | Kafka partition number |
| `msg_offset` | `BIGINT` | Kafka offset |
| `timestamp` | `BIGINT` | Message timestamp in milliseconds |

**Examples:**

```sql
-- Simple string match
value_raw LIKE '%payment_failed%'

-- Extract a JSON field (returns string)
json_extract_string(value_json, '$.status') = 'failed'

-- Numeric comparison — cast first
CAST(json_extract(value_json, '$.amount') AS DOUBLE) > 1000

-- Multiple conditions
json_extract_string(value_json, '$.country') = 'US'
  AND CAST(json_extract(value_json, '$.retries') AS INTEGER) >= 3

-- Key pattern
key LIKE 'user-%'

-- Timestamp range within the result set (ms since epoch)
timestamp > 1700000000000

-- Regex
regexp_matches(value_raw, 'error|exception', 'i')
```

> **Note:** SQL mode loads all messages in the range into DuckDB before filtering, so it scans the full `max_messages` window. Use timestamp or offset range to narrow the scan first.

---

### Key filter

Applies independently of the value filter. Supported match modes:

| Mode | Behaviour |
|---|---|
| `contains` | Case-insensitive substring — `user` matches `user-123`, `new_user` |
| `exact` | Full string must match — `user-123` only |
| `prefix` | Key must start with the value — `order-` matches `order-456` |
| `regex` | ECMAScript regular expression — `^user-\d+$` |

---

**Scope controls:** from/to timestamp, from/to offset, partition list (comma-separated IDs — or click **Filter** in the partition overview), format override, message limit.

> **Tip:** Contextual hints appear below each search field explaining the expected syntax for the active mode. Recent searches are saved per field and shown as a dropdown when you click into the input.

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
3. AdminClient resolves timestamp/offset range → per-partition start offsets (up to 8 partitions in parallel)
4. Each partition thread seeks its Consumer to the start offset and streams batches
5. Each message → deserializer (Avro / JSON / Protobuf / plain) → Python dict; key also deserialized via schema registry if Confluent-encoded
6. For **fulltext / jsonpath / key** modes: filters applied in Python per message; only matched rows kept
7. For **SQL** mode: messages inserted into an in-process DuckDB table; `WHERE` clause applied at the end
8. Results capped at `max_messages`; search stops early once the cap is reached
9. `event: progress` emitted every batch; `event: done` closes stream with final payload
10. `DELETE /api/search/{task_id}` cancels an in-flight task; on-demand message expand re-fetches the single message by partition+offset

---

## File structure

```
kafka-lens/
├── pyproject.toml              pip/pipx package definition + entry point
├── docker-compose.yml          Kafka (KRaft) + Schema Registry
├── requirements.txt
├── run.py                      uvicorn entry point (dev)
├── start.sh                    One-command bootstrap (creates .venv)
├── config/
│   └── kafka-jaas.conf         SASL credentials for the Docker broker
├── app/
│   ├── __main__.py             Entry point for `kafka-lens` CLI and `python -m app`
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
│   ├── api/
│   │   ├── clusters.py         /api/clusters CRUD + /test endpoint
│   │   ├── topics.py           /api/topics list + metadata
│   │   └── search.py           POST search → task_id; GET SSE stream; DELETE cancel
│   ├── templates/
│   │   └── index.html          Full UI (sidebar + search form + results table)
│   └── static/
│       ├── css/app.css         Custom vars, scrollbar, animations, format badges
│       └── js/app.js           Alpine.js app: SSE handler, pagination, cluster CRUD
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
