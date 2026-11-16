# KafkaLens — Claude Context Document

This file is read automatically by Claude Code at session start. It contains everything needed to pick up development without asking the user to re-explain the project.

---

## What This Is

A fast, locally-hosted Kafka message browser built as a drop-in replacement for Confluent Control Center, which crashes under Chrome memory pressure on large topics. The goal is a tool an engineer can point at any Kafka cluster (local, Docker, Confluent Cloud, AWS MSK) and instantly search, filter, and inspect messages — with schema-aware deserialization, no build step, and no external database.

**Current stable release: v0.2.41** (tagged on GitHub)  
**Repo:** `github.com/Akhil120/kafka-lens`  
**Working directory:** `/Users/akhilkondapaneni/Desktop/kafka-lens`

---

## Stack

| Layer | Choice | Why |
|---|---|---|
| Web framework | FastAPI | async, fast, good SSE support |
| Kafka client | confluent-kafka (librdkafka C lib) | 10–100× faster than kafka-python |
| SQL filter engine | DuckDB (in-process) | columnar, native JSON functions, no external service |
| Schema decoding | fastavro + protobuf + httpx (SR client) | Avro magic byte, Confluent SR wire format |
| Credential encryption | cryptography (Fernet) | AES-128 at rest in `~/.kafka-lens/` |
| Frontend | Alpine.js + Tailwind CSS (CDN) | No build step; SPA feel with zero bundler |
| Templating | Jinja2 | Server-rendered HTML, templates hot-reload without restart |
| Streaming | SSE (Server-Sent Events) | Unidirectional, auto-reconnect, simpler than WebSockets for this use case |

---

## Running the App

```bash
# Option A: pipx (production)
pipx install git+https://github.com/Akhil120/kafka-lens.git
kafka-lens   # → http://localhost:8000

# Option B: local dev (templates + JS hot-reload, Python changes need restart)
./start.sh   # creates .venv, starts uvicorn

# Option C: full Docker stack
docker compose up -d   # Kafka + Schema Registry + app
```

**Important:** The running process (`kafka-lens` or `uvicorn`) loads Python modules into memory at startup. Template and static file changes (`.html`, `.js`, `.css`) take effect on the next browser request — no restart needed. Python module changes (`engine.py`, `consumer.py`, etc.) require a process restart.

The pipx binary is at `~/.local/pipx/.cache/df43a8c45e0893e/bin/kafka-lens` but it **reads source files from the working directory** (`/Users/akhilkondapaneni/Desktop/kafka-lens/app/`), confirmed via `app.main.__file__`.

---

## File Structure

```
kafka-lens/
├── pyproject.toml              version + pip/pipx package definition
├── app/__init__.py             __version__ — bump this + pyproject.toml together
├── app/main.py                 FastAPI app, static/template mounts, passes `version` to templates
├── app/config.py               Pydantic settings
├── app/models.py               All Pydantic request/response models (SearchRequest is key)
├── app/crypto.py               Fernet key generation + encrypt/decrypt
├── app/store.py                Cluster config persistence (~/.kafka-lens/clusters.json)
├── app/kafka/
│   ├── auth.py                 Builds librdkafka config dict for all 8 auth types
│   └── consumer.py             AdminClient, topic listing, fetch_partition, fetch_one_message
├── app/schema/
│   ├── registry.py             Async HTTP client for Confluent Schema Registry
│   └── deserializer.py         Auto-detect + deserialize Avro/JSON/Protobuf/plain
├── app/search/
│   └── engine.py               Core search logic, DuckDB SQL mode, SSE emit, export
├── app/api/
│   ├── clusters.py             /api/clusters CRUD + /test endpoint
│   ├── topics.py               /api/topics list + metadata
│   └── search.py               POST search → task_id; GET SSE stream; GET export; DELETE cancel
├── app/templates/
│   ├── base.html               CDN imports (Tailwind, Alpine, hljs), theme toggle, static asset refs
│   └── index.html              Full UI — sidebar, search form, results table, side drawer, modals
├── app/static/
│   ├── css/app.css             Custom vars, scrollbar, format badges, x-cloak
│   └── js/app.js               Alpine.js app: all UI logic, SSE handler, pagination, export, drawer
├── FEATURES.md                 Feature backlog
└── CLAUDE.md                   This file
```

---

## Search Architecture

### Task lifecycle

1. `POST /api/clusters/{id}/topics/{topic}/search` → `create_task()` → returns `{task_id}`
2. `_run_search(task_id)` runs in a background thread
3. `GET /api/search/{task_id}/stream` → `stream_results()` polls `_tasks[task_id]` and emits SSE
4. `DELETE /api/search/{task_id}` → sets `cancel_event`, stops partition threads

### `_run_search` internals (`engine.py:281`)

- Calls `resolve_partition_ranges()` → per-partition `(start_offset, end_offset)` after resolving timestamps
- Spawns up to `MAX_PARTITION_THREADS = 8` threads via `ThreadPoolExecutor`
- Each thread runs `_search_partition` (fulltext/jsonpath/key) or `_search_partition_sql` (SQL mode)
- Results are merged, sorted by `(timestamp, partition, offset)`, capped at `max_messages`
- Only `_PREVIEW_BYTES = 300` chars of value are stored per match (the full value is re-fetched on row click)

### Filter modes (speed order: fast → slow)

| Mode | How it filters | Stops early? |
|---|---|---|
| **fulltext** | `filter_str in value_raw` per message | Yes — stops at `max_messages` matches |
| **jsonpath** | `json.loads()` + path traversal per message | Yes — stops at `max_messages` matches |
| **SQL** | Loads ALL messages into DuckDB, then `WHERE` clause | No — always scans full range |

**SQL mode gotcha:** SQL never stops early. Always pair it with a timestamp or offset range, otherwise it scans the entire topic. Fulltext is fastest because there's no JSON parsing.

### Message detail fetch (`engine.py:394`)

`fetch_message_detail` is called when a user clicks a row in the results table. It calls `fetch_one_message` (new Consumer → assign → poll → close) to get the full value. The preview stored during search is only 300 chars.

### Export (`engine.py:494`)

`iter_export` re-fetches all matched messages from Kafka for JSON/CSV download. Uses `asyncio.gather` + `asyncio.Semaphore(16)` to run 16 `fetch_one_message` calls concurrently instead of sequentially. This was ~16× faster than the original sequential loop (500 messages: ~8s vs ~50s).

Export endpoints: `GET /api/search/{task_id}/export.json` and `.../export.csv`

---

## Frontend Architecture (`app.js`)

Single Alpine.js component `kafkaApp()` manages all UI state. Key state:

```javascript
activeClusters: {}        // clusterId -> { expanded, topics, loading }
selectedCluster / selectedTopic
messages: []              // matched MessageSummary objects (preview only)
expandedRow: null         // index into pagedMessages() — drives side drawer
expandedDetail: {}        // key: `${partition}-${offset}` -> full KafkaMessage (cached)
expandedDetailLoading     // spinner state for drawer fetch
copied: null              // ID string of last-copied button (cleared after 2s)
taskId                    // current/last search task ID
searching                 // SSE in-flight
```

Key methods:
- `startSearch()` — POST to search API, opens SSE via `EventSource`
- `toggleRow(idx)` — opens/closes side drawer; fetches full message if not cached
- `selectedMsg()` / `selectedDetail()` — computed helpers for the drawer
- `copyToClipboard(text, id)` — writes to clipboard; sets `copied = id` for 2s feedback
- `exportJson()` / `exportCsv()` — trigger download via hidden `<a>` click (avoids popup blocker)
- `_triggerDownload(url, filename)` — creates temporary `<a>`, clicks it, removes it

### Side drawer

The message detail panel is a fixed right-side overlay (`#detail-panel`, `w-[580px]`). It does **not** expand inline in the table — the table rows never shift. Sections: purple accent bar, header (P/offset, format, timestamp, ESC hint), Metadata card (topic/format/partition/offset/timestamp), Key, Kafka Headers, Value (syntax-highlighted).

Opened by `toggleRow(idx)`. Closed by: clicking ×, pressing ESC (`@keydown.escape.window`), or clicking the semi-transparent backdrop.

### Cache-busting

`base.html` appends `?v={{ version }}` to `app.css` and `app.js` URLs. This ensures browsers fetch fresh static files after every version bump — critical fix for Windows users who saw stale UI (partition overview missing, old JS cached).

---

## Version Bumping Convention

Always bump **both** files together:
- `app/__init__.py` → `__version__ = "0.2.X"`
- `pyproject.toml` → `version = "0.2.X"`

Then commit, tag (`git tag -a vX.Y.Z -m "..."`) and push tag separately (`git push origin vX.Y.Z`).

---

## Authentication Types

All handled in `app/kafka/auth.py` as a dict passed to `confluent_kafka.Consumer/AdminClient`:

| Protocol | Mechanism | Use case |
|---|---|---|
| PLAINTEXT | — | Local Docker dev |
| SASL_PLAINTEXT | PLAIN / SCRAM-256 / SCRAM-512 | On-prem with auth |
| SSL | — | mTLS; CA/cert/key file paths |
| SASL_SSL | PLAIN | Confluent Cloud (API key + secret) |
| SASL_SSL | SCRAM-256/512 | SCRAM over TLS |
| SASL_SSL | OAUTHBEARER | OIDC/OAuth |

---

## Bugs Fixed and Decisions Made

### 1. DuckDB scan cache reverted (v0.2.38)
**What was tried:** A DuckDB-backed message cache that pre-fetched all messages into an in-memory table so repeated searches on the same topic were instant.  
**Why reverted:** The cache always consumed the full `max_messages` window before filtering. Streaming (fulltext/jsonpath) stops as soon as `max_messages` *matches* are found — often scanning far fewer messages. For 80k messages at max_messages=1000, cache took 130s vs streaming's ~5s.  
**Decision:** DuckDB is only used for SQL filter mode where you genuinely need full-table scans.

### 2. Consumer pool reverted (v0.2.38)
**What was tried:** A pool of persistent `Consumer` objects keyed by cluster ID to avoid SASL handshake overhead on every fetch.  
**Why reverted:** Added complexity, locking, and didn't measurably help the common case (search partition threads already create short-lived consumers). Caused subtle bugs when pool consumers were reused across searches.  
**Decision:** Each `fetch_partition` and `fetch_one_message` call creates its own Consumer. Simple, correct, and fast enough.

### 3. Per-message lock contention
**What happened:** An early version of the DuckDB cache acquired a lock inside the per-message loop across 8 partition threads — 80,000 lock acquisitions for 10k messages. This was catastrophically slow.  
**Fix:** Lock once per batch (~40 acquisitions) to update byte totals and flush to DuckDB.  
**Later:** Entire cache reverted anyway (see above).

### 4. Export slowness (fixed in v0.2.39)
**Root cause:** `iter_export` was sequential — one `fetch_one_message` per matched message, each creating a new Kafka Consumer (SASL handshake + seek + poll + close). 500 messages × ~100ms = ~50s.  
**Fix:** `asyncio.gather` + `asyncio.Semaphore(16)` runs 16 fetches concurrently. 500 messages now takes ~8s.  
**Location:** `engine.py:494`, constant `_EXPORT_CONCURRENCY = 16`

### 5. Export download blocked by popup blocker (fixed in v0.2.40)
**Root cause:** `window.open(url)` is classified as a popup by browsers, especially when triggered programmatically.  
**Fix:** `_triggerDownload()` creates a hidden `<a download>` element, clicks it, removes it. Never blocked.

### 6. Inline expanded row replaced with side drawer (v0.2.40)
**Problem:** Clicking a row inserted an expanded `<tr>` below it, pushing all subsequent rows down. Disorienting when scanning many results.  
**Fix:** Fixed right-side overlay panel. Table rows never move. Selected row gets a purple left-border highlight.

### 7. Windows users seeing stale UI (fixed in v0.2.41)
**Root cause:** `app.js` and `app.css` were served without cache-busting. After `pipx upgrade`, Windows browsers kept serving the old files, causing features (partition overview, drawer, etc.) to appear broken.  
**Fix:** `?v={{ version }}` appended to static asset URLs in `base.html`. Each release busts the cache automatically.

### 8. Partition overview absent on Windows (pre-v0.2.41)
**Root cause:** Same as #7 — stale cached `app.js` from before partition overview was added.  
**Workaround at the time:** Hard refresh (`Ctrl+Shift+R`). Permanently fixed by cache-busting.

---

## Known Limitations / Future Work

See `FEATURES.md` for the full backlog. Key items:

- **"Scope to ±5 min" button** — in the message drawer, pre-fills From/To Timestamp around the selected message's timestamp. Useful workflow: find a representative message, then narrow the search window around it.

- **SQL mode always full-scans** — no early-exit. Mitigate by setting timestamp/offset ranges before running SQL.

- **`max_messages` is a match cap, not a scan cap** — for selective filters, the actual number of messages consumed can be `max_messages × (1 / match_rate)`. A filter that matches 1-in-1000 with `max_messages=1000` will scan up to 1M messages.

- **Export waits for all fetches before streaming** — `asyncio.gather` collects all results before yielding. For very large exports (>1000 messages), there's a noticeable wait before the download starts. Could be improved with chunked streaming.

---

## Credential Storage

- Key file: `~/.kafka-lens/key` — Fernet 32-byte key, generated once on first run
- Config: `~/.kafka-lens/clusters.json` — encrypted fields: `password`, `schema_registry_password`, `ssl_key_password`
- Cluster ID is a UUID; display name is user-defined
- Configs survive `pipx upgrade` (stored in home dir, not package dir)

---

## Testing

```bash
# Requires app on :8000, Kafka on :9092, Schema Registry on :8081
# Topic "analytics.events" must exist and contain messages
.venv/bin/pip install playwright pytest-playwright httpx
.venv/bin/playwright install chromium
.venv/bin/pytest tests/test_ui.py -v   # 35 tests
```
