from __future__ import annotations
import asyncio
import json
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import AsyncGenerator, Any

import duckdb
import orjson

from app.models import ClusterConfig, SearchRequest, KafkaMessage, MessageSummary
from app.kafka.consumer import fetch_partition, resolve_partition_ranges, fetch_one_message
from app.schema.deserializer import make_deserializer

_tasks: dict[str, dict] = {}

MAX_PARTITION_THREADS = 8
_PREVIEW_BYTES = 300  # chars stored per matched message for the results table
_EXPORT_CONCURRENCY = 16  # parallel fetch_one_message calls during export


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


def _run_search(task_id: str) -> None:
    task = _tasks.get(task_id)
    if task is None:
        return
    cluster: ClusterConfig = task["cluster"]
    topic: str = task["topic"]
    request: SearchRequest = task["request"]
    cancel_event: threading.Event = task["cancel"]

    all_matched: list[dict] = []

    try:
        partition_ranges = resolve_partition_ranges(cluster, topic, request)
        if not partition_ranges:
            return

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

    except Exception as e:
        with task["lock"]:
            task["error"] = str(e)
    finally:
        with task["lock"]:
            task["done"] = True


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
    sem = asyncio.Semaphore(_EXPORT_CONCURRENCY)

    async def fetch_full(s: dict) -> dict | None:
        async with sem:
            raw = await loop.run_in_executor(
                None, fetch_one_message, cluster, topic, s["partition"], s["offset"]
            )
        if raw is None:
            return None
        val, msg_fmt = deserializer.deserialize(raw["value_bytes"], raw["key_bytes"], request.format)
        if isinstance(val, (dict, list)):
            value_raw = orjson.dumps(val).decode("utf-8", errors="replace")
        else:
            value_raw = str(val) if val is not None else ""
        key = deserializer.deserialize_key(raw["key_bytes"])
        return {
            "partition": raw["partition"],
            "offset": raw["offset"],
            "timestamp_human": _ts_to_human(raw["timestamp"]),
            "key": key,
            "value_raw": value_raw,
            "headers": raw["headers"],
            "format": msg_fmt,
        }

    messages = await asyncio.gather(*[fetch_full(s) for s in summaries])
    messages = [m for m in messages if m is not None]

    if fmt == "csv":
        yield b"partition,offset,timestamp_human,key,value_raw\r\n"
        for m in messages:
            row = ",".join(json.dumps(x) for x in [m["partition"], m["offset"], m["timestamp_human"], m["key"] or "", m["value_raw"]])
            yield (row + "\r\n").encode()
    else:
        yield b"["
        first = True
        for m in messages:
            if not first:
                yield b","
            yield orjson.dumps(m)
            first = False
        yield b"]"
