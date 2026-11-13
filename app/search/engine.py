from __future__ import annotations
import asyncio
import json
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import AsyncGenerator, Any

import duckdb
import orjson

from app.models import ClusterConfig, SearchRequest, KafkaMessage, MessageSummary
from app.kafka.consumer import fetch_partition, resolve_partition_ranges, fetch_one_message
from app.schema.deserializer import make_deserializer

import logging
log = logging.getLogger(__name__)

_tasks: dict[str, dict] = {}

MAX_PARTITION_THREADS = 8
_PREVIEW_BYTES = 300  # chars stored per matched message for the results table

# ── DuckDB scan cache ─────────────────────────────────────────────────────────
# For ranges ≤ _CACHE_MAX_SCAN_SIZE messages: fetch everything into an in-memory
# DuckDB table, cache it for _CACHE_TTL seconds, and re-filter on cache hit.
# Large ranges fall back to the streaming path (fetch+filter+stop at max_messages).

_CACHE_MAX_SCAN_SIZE = 500_000   # messages; skip caching above this estimate
_CACHE_TTL          = 600.0      # seconds a cached scan stays alive
_CACHE_MAX_ENTRIES  = 6          # max simultaneous cached scans


@dataclass
class _ScanCache:
    con:         duckdb.DuckDBPyConnection
    total_rows:  int
    expires:     float
    lock:        threading.Lock = field(default_factory=threading.Lock)


_scan_caches:     dict[str, _ScanCache] = {}
_scan_cache_lock: threading.Lock = threading.Lock()


def _cache_key(cluster_id: str, topic: str, ranges: dict[int, tuple[int, int]]) -> str:
    return f"{cluster_id}:{topic}:{sorted(ranges.items())}"


def _estimated_size(ranges: dict[int, tuple[int, int]]) -> int:
    return sum(max(0, e - s) for s, e in ranges.values())


def _get_cached(cluster_id: str, topic: str, ranges: dict[int, tuple[int, int]]) -> _ScanCache | None:
    key = _cache_key(cluster_id, topic, ranges)
    with _scan_cache_lock:
        entry = _scan_caches.get(key)
        if entry is None:
            return None
        if time.monotonic() > entry.expires:
            _scan_caches.pop(key, None)
            try:
                entry.con.close()
            except Exception:
                pass
            return None
        return entry


def _store_cached(
    cluster_id: str, topic: str, ranges: dict[int, tuple[int, int]],
    con: duckdb.DuckDBPyConnection, total_rows: int,
) -> _ScanCache:
    key = _cache_key(cluster_id, topic, ranges)
    entry = _ScanCache(con=con, total_rows=total_rows, expires=time.monotonic() + _CACHE_TTL)
    with _scan_cache_lock:
        # Evict oldest if at capacity
        if len(_scan_caches) >= _CACHE_MAX_ENTRIES:
            oldest = min(_scan_caches, key=lambda k: _scan_caches[k].expires)
            evicted = _scan_caches.pop(oldest)
            try:
                evicted.con.close()
            except Exception:
                pass
        # Another thread may have cached same key while we fetched — prefer theirs.
        existing = _scan_caches.get(key)
        if existing and time.monotonic() < existing.expires:
            try:
                con.close()
            except Exception:
                pass
            return existing
        _scan_caches[key] = entry
    return entry


def create_task(cluster: ClusterConfig, topic: str, request: SearchRequest) -> str:
    # Clean up completed tasks before adding a new one; keeps _tasks from growing unbounded.
    for tid in list(_tasks):
        t = _tasks[tid]
        if t.get("done") or t.get("error"):
            t["cancel"].set()
            _tasks.pop(tid, None)

    task_id = str(uuid.uuid4())
    cancel_event = threading.Event()
    _tasks[task_id] = {
        "cluster": cluster,
        "topic": topic,
        "request": request,
        "cancel": cancel_event,
        "results": [],       # list of MessageSummary-like dicts — lightweight
        "scanned": 0,
        "matched": 0,
        "done": False,
        "error": None,
        "lock": threading.Lock(),
        "results_capped": False,
    }
    t = threading.Thread(target=_run_search, args=(task_id,), daemon=True)
    t.start()
    return task_id


def cancel_task(task_id: str) -> bool:
    task = _tasks.get(task_id)
    if task:
        task["cancel"].set()
        _tasks.pop(task_id, None)
        return True
    return False


def _jsonpath_get(val: Any, path: str) -> Any:
    """Navigate a simple $.a.b[0].c JSONPath into a dict/list."""
    parts = re.split(r'[.\[\]]+', path.lstrip('$').lstrip('.'))
    current = val
    for part in parts:
        if not part:
            continue
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            current = current[int(part)]
        else:
            return None
    return current


def _matches_filters(
    key: str | None,
    value_raw: str,
    val: Any,
    request: SearchRequest,
) -> bool:
    """Return True if the message passes all key/value filters (non-SQL modes)."""
    if request.key_filter:
        kf = request.key_filter
        km = request.key_match
        if key is None:
            return False
        if km == "exact" and key != kf:
            return False
        elif km == "prefix" and not key.startswith(kf):
            return False
        elif km == "regex" and not re.search(kf, key):
            return False
        elif km == "contains" and kf.lower() not in key.lower():
            return False

    if request.value_filter:
        vf = request.value_filter
        vm = request.value_filter_mode
        if vm == "jsonpath":
            path, _, expected = vf.partition("=")
            path = path.strip()
            expected = expected.strip().strip("\"'")
            actual = _jsonpath_get(val, path)
            if expected:
                if str(actual) != expected:
                    return False
            elif actual is None:
                return False
        else:  # fulltext
            if vf.lower() not in value_raw.lower():
                return False

    return True


def _search_partition(
    cluster: ClusterConfig,
    topic: str,
    pid: int,
    start_offset: int,
    end_offset: int,
    max_messages: int,
    request: SearchRequest,
    cancel_event: threading.Event,
    task: dict,
) -> list[dict]:
    deserializer = make_deserializer(
        cluster.schema_registry_url,
        cluster.schema_registry_username,
        cluster.schema_registry_password,
        request.schema_registry_url,
    )

    use_sql = request.value_filter_mode == "sql" and request.value_filter

    if use_sql:
        return _search_partition_sql(
            cluster, topic, pid, start_offset, end_offset, max_messages,
            request, cancel_event, task, deserializer,
        )

    # Raw-bytes pre-filter for fulltext: skip deserialization when term provably absent.
    _use_prefilter = (
        request.value_filter_mode == "fulltext"
        and bool(request.value_filter)
        and not request.key_filter
    )
    _needle: bytes | None = request.value_filter.encode("utf-8", errors="replace") if _use_prefilter else None

    matched: list[dict] = []
    for batch in fetch_partition(
        cluster, topic, pid, start_offset, end_offset, max_messages,
        lambda: cancel_event.is_set(), batch_size=100,
    ):
        if cancel_event.is_set():
            break

        for raw in batch:
            # Pre-filter: skip Avro magic-byte messages (varints not UTF-8 text).
            if _needle is not None:
                vb = raw["value_bytes"] or b""
                if vb and vb[0] != 0x00 and _needle not in vb:
                    raw["value_bytes"] = None
                    continue

            val, fmt = deserializer.deserialize(
                raw["value_bytes"], raw["key_bytes"], request.format
            )
            raw["value_bytes"] = None  # release raw bytes immediately

            key = deserializer.deserialize_key(raw["key_bytes"])

            if isinstance(val, (dict, list)):
                value_raw = orjson.dumps(val).decode("utf-8", errors="replace")
            else:
                value_raw = str(val) if val is not None else ""

            # Filter with the full value so matches in large payloads are never missed.
            if not _matches_filters(key, value_raw, val, request):
                del val
                continue

            # Store only a short preview — the full value is fetched on-demand from Kafka.
            preview = value_raw[:_PREVIEW_BYTES]
            del val
            del value_raw

            matched.append({
                "partition": raw["partition"],
                "offset": raw["offset"],
                "timestamp": raw["timestamp"],
                "timestamp_human": _ts_to_human(raw["timestamp"]),
                "key": key,
                "format": fmt,
                "preview": preview,
                "headers": raw["headers"],
            })

        with task["lock"]:
            task["scanned"] += len(batch)

    return matched


def _search_partition_sql(
    cluster: ClusterConfig,
    topic: str,
    pid: int,
    start_offset: int,
    end_offset: int,
    max_messages: int,
    request: SearchRequest,
    cancel_event: threading.Event,
    task: dict,
    deserializer,
) -> list[dict]:
    """SQL mode: accumulate all messages in an in-process DuckDB then apply WHERE clause."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("""
            CREATE TABLE messages (
                partition INTEGER,
                msg_offset BIGINT,
                timestamp BIGINT,
                key VARCHAR,
                value_json JSON,
                value_raw VARCHAR,
                headers VARCHAR,
                fmt VARCHAR
            )
        """)

        where_clauses, params = _build_where(request)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        for batch in fetch_partition(
            cluster, topic, pid, start_offset, end_offset, max_messages,
            lambda: cancel_event.is_set(), batch_size=500,
        ):
            if cancel_event.is_set():
                break

            rows = []
            for raw in batch:
                val, fmt = deserializer.deserialize(
                    raw["value_bytes"], raw["key_bytes"], request.format
                )
                raw["value_bytes"] = None
                key = deserializer.deserialize_key(raw["key_bytes"])

                if isinstance(val, (dict, list)):
                    value_json = orjson.dumps(val).decode("utf-8", errors="replace")
                    value_raw = value_json
                else:
                    value_raw = str(val) if val is not None else ""
                    value_json = None

                rows.append((
                    raw["partition"], raw["offset"], raw["timestamp"],
                    key, value_json, value_raw, orjson.dumps(raw["headers"]).decode(), fmt,
                ))

            if rows:
                con.executemany("INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
                with task["lock"]:
                    task["scanned"] += len(rows)

        matched_rows = con.execute(
            f"SELECT * FROM messages {where_sql} ORDER BY timestamp, partition, msg_offset",
            params,
        ).fetchall()
        return [_row_to_summary(row) for row in matched_rows]
    finally:
        con.close()


_CREATE_MSG_TABLE = """
CREATE TABLE messages (
    partition  INTEGER,
    msg_offset BIGINT,
    timestamp  BIGINT,
    key        VARCHAR,
    value_raw  VARCHAR,
    value_json JSON,
    headers    VARCHAR,
    fmt        VARCHAR
)
"""


def _fetch_all_to_duckdb(
    cluster: ClusterConfig,
    topic: str,
    partition_ranges: dict[int, tuple[int, int]],
    request: SearchRequest,
    cancel_event: threading.Event,
    task: dict,
) -> duckdb.DuckDBPyConnection:
    """Fetch ALL messages in partition_ranges into an in-memory DuckDB table.

    Each partition thread accumulates rows locally then bulk-inserts under a
    lock so the DuckDB connection is never touched concurrently.
    """
    con = duckdb.connect(":memory:")
    con.execute(_CREATE_MSG_TABLE)
    insert_lock = threading.Lock()

    def _fetch_one_partition(pid: int, start: int, end: int) -> None:
        deserializer = make_deserializer(
            cluster.schema_registry_url,
            cluster.schema_registry_username,
            cluster.schema_registry_password,
            request.schema_registry_url,
        )
        rows: list[tuple] = []
        # No max_messages cap here — we want the full range for caching.
        for batch in fetch_partition(
            cluster, topic, pid, start, end, 10_000_000,
            lambda: cancel_event.is_set(), batch_size=2000,
        ):
            if cancel_event.is_set():
                break
            for raw in batch:
                val, fmt = deserializer.deserialize(raw["value_bytes"], raw["key_bytes"], request.format)
                raw["value_bytes"] = None
                key = deserializer.deserialize_key(raw["key_bytes"])
                if isinstance(val, (dict, list)):
                    value_raw = orjson.dumps(val).decode("utf-8", errors="replace")
                    value_json = value_raw
                else:
                    value_raw = str(val) if val is not None else ""
                    value_json = None
                rows.append((
                    raw["partition"], raw["offset"], raw["timestamp"],
                    key, value_raw, value_json,
                    orjson.dumps(raw["headers"]).decode(), fmt,
                ))
            with task["lock"]:
                task["scanned"] += len(batch)

        if rows:
            with insert_lock:
                con.executemany("INSERT INTO messages VALUES (?,?,?,?,?,?,?,?)", rows)

    n = len(partition_ranges)
    with ThreadPoolExecutor(max_workers=min(n, MAX_PARTITION_THREADS)) as pool:
        futures = [
            pool.submit(_fetch_one_partition, pid, start, end)
            for pid, (start, end) in partition_ranges.items()
        ]
        for f in as_completed(futures):
            if cancel_event.is_set():
                break
            f.result()   # propagate any exception

    return con


def _query_duckdb_cache(
    entry: _ScanCache,
    request: SearchRequest,
) -> tuple[list[dict], bool]:
    """Run search filters against a cached DuckDB table, return (summaries, capped)."""
    clauses: list[str] = []
    params:  list[Any] = []

    if request.key_filter:
        kf, km = request.key_filter, request.key_match
        if km == "exact":
            clauses.append("key = ?"); params.append(kf)
        elif km == "prefix":
            clauses.append("key LIKE ?"); params.append(f"{kf}%")
        elif km == "regex":
            clauses.append("regexp_matches(key, ?)"); params.append(kf)
        else:
            clauses.append("key ILIKE ?"); params.append(f"%{kf}%")

    if request.value_filter:
        vf, vm = request.value_filter, request.value_filter_mode
        if vm == "jsonpath":
            path, _, expected = vf.partition("=")
            path, expected = path.strip(), expected.strip().strip("\"'")
            if expected:
                clauses.append("json_extract_string(value_json, ?) = ?")
                params.extend([path, expected])
            else:
                clauses.append("json_extract_string(value_json, ?) IS NOT NULL")
                params.append(path)
        elif vm == "sql":
            clauses.append(f"({vf})")
        else:
            clauses.append("value_raw ILIKE ?"); params.append(f"%{vf}%")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    # Fetch one extra row so we can detect capping without a separate COUNT().
    limit = request.max_messages + 1
    sql = (
        f"SELECT partition, msg_offset, timestamp, key, value_raw, headers, fmt "
        f"FROM messages {where} "
        f"ORDER BY timestamp, partition, msg_offset "
        f"LIMIT ?"
    )
    params.append(limit)

    with entry.lock:
        rows = entry.con.execute(sql, params).fetchall()

    capped = len(rows) > request.max_messages
    rows = rows[:request.max_messages]

    summaries = [{
        "partition": r[0],
        "offset":    r[1],
        "timestamp": r[2],
        "timestamp_human": _ts_to_human(r[2]),
        "key":       r[3],
        "preview":   (r[4] or "")[:_PREVIEW_BYTES],
        "headers":   json.loads(r[5]) if r[5] else {},
        "format":    r[6],
    } for r in rows]

    return summaries, capped


def _run_search(task_id: str) -> None:
    task = _tasks.get(task_id)
    if task is None:
        return
    cluster: ClusterConfig = task["cluster"]
    topic: str = task["topic"]
    request: SearchRequest = task["request"]
    cancel_event: threading.Event = task["cancel"]

    try:
        partition_ranges = resolve_partition_ranges(cluster, topic, request)
        if not partition_ranges:
            return

        est = _estimated_size(partition_ranges)
        use_cache = est <= _CACHE_MAX_SCAN_SIZE

        if use_cache:
            _run_search_cached(task_id, task, cluster, topic, request, partition_ranges, cancel_event)
        else:
            log.info("scan cache skipped for %s/%s — estimated %d messages (limit %d)",
                     cluster.name, topic, est, _CACHE_MAX_SCAN_SIZE)
            _run_search_streaming(task_id, task, cluster, topic, request, partition_ranges, cancel_event)

    except Exception as e:
        with task["lock"]:
            task["error"] = str(e)
    finally:
        with task["lock"]:
            task["done"] = True


def _run_search_cached(
    task_id: str,
    task: dict,
    cluster: ClusterConfig,
    topic: str,
    request: SearchRequest,
    partition_ranges: dict[int, tuple[int, int]],
    cancel_event: threading.Event,
) -> None:
    """Cache path: fetch once into DuckDB, re-filter on subsequent hits."""
    entry = _get_cached(cluster.id, topic, partition_ranges)

    if entry is not None:
        log.info("scan cache HIT for %s/%s (%d rows)", cluster.name, topic, entry.total_rows)
        with task["lock"]:
            task["scanned"] = entry.total_rows
        summaries, capped = _query_duckdb_cache(entry, request)
        with task["lock"]:
            task["results"] = summaries
            task["matched"] = len(summaries)
            task["results_capped"] = capped
        return

    log.info("scan cache MISS for %s/%s — fetching %d partitions",
             cluster.name, topic, len(partition_ranges))
    con = _fetch_all_to_duckdb(cluster, topic, partition_ranges, request, cancel_event, task)

    if cancel_event.is_set():
        try:
            con.close()
        except Exception:
            pass
        return

    with task["lock"]:
        total_rows = task["scanned"]

    entry = _store_cached(cluster.id, topic, partition_ranges, con, total_rows)
    summaries, capped = _query_duckdb_cache(entry, request)
    with task["lock"]:
        task["results"] = summaries
        task["matched"] = len(summaries)
        task["results_capped"] = capped


def _run_search_streaming(
    task_id: str,
    task: dict,
    cluster: ClusterConfig,
    topic: str,
    request: SearchRequest,
    partition_ranges: dict[int, tuple[int, int]],
    cancel_event: threading.Event,
) -> None:
    """Streaming path for large ranges: fetch+filter+stop at max_messages."""
    all_matched: list[dict] = []
    n_partitions = len(partition_ranges)

    with ThreadPoolExecutor(max_workers=min(n_partitions, MAX_PARTITION_THREADS)) as pool:
        futures = {
            pool.submit(
                _search_partition,
                cluster, topic, pid, start, end, request.max_messages,
                request, cancel_event, task,
            ): pid
            for pid, (start, end) in partition_ranges.items()
        }

        for future in as_completed(futures):
            if cancel_event.is_set():
                break
            try:
                partition_results = future.result()
                all_matched.extend(partition_results)
                all_matched.sort(key=lambda m: (m["timestamp"], m["partition"], m["offset"]))
                if len(all_matched) > request.max_messages:
                    all_matched = all_matched[:request.max_messages]
                    cancel_event.set()
                    with task["lock"]:
                        task["results"] = list(all_matched)
                        task["matched"] = len(all_matched)
                        task["results_capped"] = True
                else:
                    with task["lock"]:
                        task["results"] = list(all_matched)
                        task["matched"] = len(all_matched)
            except Exception as e:
                with task["lock"]:
                    task["error"] = str(e)


def _row_to_summary(row: tuple) -> dict:
    """Convert a DuckDB SQL-mode row to a lightweight summary dict."""
    partition, msg_offset, ts, key, value_json, value_raw, headers_str, fmt = row
    preview = (value_raw or "")[:_PREVIEW_BYTES]
    return {
        "partition": partition,
        "offset": msg_offset,
        "timestamp": ts,
        "timestamp_human": _ts_to_human(ts),
        "key": key,
        "format": fmt,
        "preview": preview,
        "headers": json.loads(headers_str) if headers_str else {},
    }


def _build_where(request: SearchRequest) -> tuple[list[str], list]:
    clauses: list[str] = []
    params: list[Any] = []

    if request.key_filter:
        kf = request.key_filter
        km = request.key_match
        if km == "exact":
            clauses.append("key = ?")
            params.append(kf)
        elif km == "prefix":
            clauses.append("key LIKE ?")
            params.append(f"{kf}%")
        elif km == "regex":
            clauses.append("regexp_matches(key, ?)")
            params.append(kf)
        else:
            clauses.append("key LIKE ?")
            params.append(f"%{kf}%")

    if request.value_filter:
        vf = request.value_filter
        vm = request.value_filter_mode
        if vm == "sql":
            clauses.append(f"({vf})")

    return clauses, params


def _ts_to_human(ts_ms: int) -> str:
    if ts_ms and ts_ms > 0:
        try:
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            pass
    return "—"


def fetch_message_detail(
    cluster: ClusterConfig,
    topic: str,
    partition: int,
    offset: int,
    request_format: str,
    schema_registry_url: str | None = None,
) -> KafkaMessage | None:
    """Re-fetch a single message from Kafka and deserialize it fully."""
    raw = fetch_one_message(cluster, topic, partition, offset)
    if raw is None:
        return None

    deserializer = make_deserializer(
        cluster.schema_registry_url,
        cluster.schema_registry_username,
        cluster.schema_registry_password,
        schema_registry_url,
    )
    val, fmt = deserializer.deserialize(raw["value_bytes"], raw["key_bytes"], request_format)

    if isinstance(val, (dict, list)):
        value_raw = orjson.dumps(val).decode("utf-8", errors="replace")
    else:
        value_raw = str(val) if val is not None else ""

    key = deserializer.deserialize_key(raw["key_bytes"])

    return KafkaMessage(
        partition=raw["partition"],
        offset=raw["offset"],
        timestamp=raw["timestamp"],
        timestamp_human=_ts_to_human(raw["timestamp"]),
        key=key,
        value=val if isinstance(val, (dict, list)) else value_raw,
        value_raw=value_raw,
        headers=raw["headers"],
        format=fmt,
    )


async def stream_results(task_id: str) -> AsyncGenerator[str, None]:
    task = _tasks.get(task_id)
    if not task:
        yield f"event: error\ndata: {json.dumps({'error': 'Task not found'})}\n\n"
        return

    last_scanned = -1
    last_matched = -1
    last_results_count = 0
    last_results_t = 0.0

    while True:
        now = time.monotonic()
        need_results = (last_matched > last_results_count and now - last_results_t >= 1.0)

        with task["lock"]:
            scanned = task["scanned"]
            matched = task["matched"]
            done = task["done"]
            error = task["error"]
            if need_results or done:
                results = list(task["results"])
            else:
                results = None

        if scanned != last_scanned or matched != last_matched:
            yield f"event: progress\ndata: {orjson.dumps({'scanned': scanned, 'matched': matched}).decode()}\n\n"
            last_scanned = scanned
            last_matched = matched

        now = time.monotonic()
        if results is not None and matched > last_results_count and now - last_results_t >= 1.0:
            # Summaries are tiny (<300 bytes each) — no need to cap or throttle heavily.
            yield f"event: results\ndata: {orjson.dumps(results, option=orjson.OPT_NON_STR_KEYS).decode()}\n\n"
            last_results_count = matched
            last_results_t = now

        if error:
            yield f"event: error\ndata: {orjson.dumps({'error': error}).decode()}\n\n"
            break

        if done:
            if results is None:
                with task["lock"]:
                    results = list(task["results"])
            with task["lock"]:
                results_capped = task["results_capped"]
            payload = {
                "scanned": scanned,
                "matched": matched,
                "messages": results,
                "results_capped": results_capped,
            }
            yield f"event: done\ndata: {orjson.dumps(payload, option=orjson.OPT_NON_STR_KEYS).decode()}\n\n"
            break

        await asyncio.sleep(0.05)


async def iter_export(task_id: str, fmt: str) -> AsyncGenerator[bytes, None]:
    """Stream all matched messages re-fetched from Kafka as JSON or CSV."""
    task = _tasks.get(task_id)
    if not task:
        return

    summaries: list[dict] = []
    with task["lock"]:
        summaries = list(task["results"])

    cluster: ClusterConfig = task["cluster"]
    topic: str = task["topic"]
    request: SearchRequest = task["request"]
    deserializer = make_deserializer(
        cluster.schema_registry_url,
        cluster.schema_registry_username,
        cluster.schema_registry_password,
        request.schema_registry_url,
    )

    loop = asyncio.get_event_loop()

    if fmt == "csv":
        yield b"partition,offset,timestamp_human,key,value_raw\r\n"

    first = True
    if fmt == "json":
        yield b"["

    for s in summaries:
        raw = await loop.run_in_executor(
            None, fetch_one_message, cluster, topic, s["partition"], s["offset"]
        )
        if raw is None:
            continue

        val, msg_fmt = deserializer.deserialize(raw["value_bytes"], raw["key_bytes"], request.format)
        if isinstance(val, (dict, list)):
            value_raw = orjson.dumps(val).decode("utf-8", errors="replace")
        else:
            value_raw = str(val) if val is not None else ""

        key = deserializer.deserialize_key(raw["key_bytes"])

        if fmt == "json":
            msg_dict = {
                "partition": raw["partition"],
                "offset": raw["offset"],
                "timestamp_human": _ts_to_human(raw["timestamp"]),
                "key": key,
                "value_raw": value_raw,
                "headers": raw["headers"],
                "format": msg_fmt,
            }
            if not first:
                yield b","
            yield orjson.dumps(msg_dict)
            first = False
        else:
            ts_human = _ts_to_human(raw["timestamp"])
            row = ",".join(json.dumps(x) for x in [raw["partition"], raw["offset"], ts_human, key or "", value_raw])
            yield (row + "\r\n").encode()

    if fmt == "json":
        yield b"]"
