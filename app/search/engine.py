from __future__ import annotations
import asyncio
import json
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import AsyncGenerator, Any

import duckdb
import orjson

from app.models import ClusterConfig, SearchRequest, KafkaMessage
from app.kafka.consumer import fetch_partition, resolve_partition_ranges
from app.schema.deserializer import make_deserializer

_tasks: dict[str, dict] = {}

MAX_PARTITION_THREADS = 8


def create_task(cluster: ClusterConfig, topic: str, request: SearchRequest) -> str:
    task_id = str(uuid.uuid4())
    cancel_event = threading.Event()
    _tasks[task_id] = {
        "cluster": cluster,
        "topic": topic,
        "request": request,
        "cancel": cancel_event,
        "results": [],
        "scanned": 0,
        "matched": 0,
        "done": False,
        "error": None,
        "lock": threading.Lock(),
    }
    t = threading.Thread(target=_run_search, args=(task_id,), daemon=True)
    t.start()
    return task_id


def cancel_task(task_id: str) -> bool:
    task = _tasks.get(task_id)
    if task:
        task["cancel"].set()
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
) -> list[KafkaMessage]:
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

    # Fast path: Python-based filtering — no DuckDB round-trip per batch
    # Pre-compute bytes needle for raw pre-filter (fulltext, non-Avro only)
    _use_prefilter = (
        request.value_filter_mode == "fulltext"
        and bool(request.value_filter)
        and not request.key_filter  # key_filter needs key decoded anyway
    )
    _needle: bytes | None = request.value_filter.encode("utf-8", errors="replace") if _use_prefilter else None

    matched: list[KafkaMessage] = []
    for batch in fetch_partition(
        cluster, topic, pid, start_offset, end_offset, max_messages,
        lambda: cancel_event.is_set(), batch_size=50000,
    ):
        if cancel_event.is_set():
            break

        for raw in batch:
            # Raw bytes pre-filter: skip deserialization when term provably absent.
            # Avro schema-registry messages start with magic byte 0x00 — skip pre-filter
            # there since integers are stored as varints (not UTF-8 text).
            if _needle is not None:
                vb = raw["value_bytes"] or b""
                if vb and vb[0] != 0x00 and _needle not in vb:
                    continue

            val, fmt = deserializer.deserialize(
                raw["value_bytes"], raw["key_bytes"], request.format
            )
            key = raw["key_bytes"].decode("utf-8", errors="replace") if raw["key_bytes"] else None

            if isinstance(val, (dict, list)):
                value_raw = orjson.dumps(val).decode("utf-8", errors="replace")
            else:
                value_raw = str(val) if val is not None else ""

            if not _matches_filters(key, value_raw, val, request):
                continue

            matched.append(KafkaMessage(
                partition=raw["partition"],
                offset=raw["offset"],
                timestamp=raw["timestamp"],
                timestamp_human=_ts_to_human(raw["timestamp"]),
                key=key,
                value=val if isinstance(val, (dict, list)) else value_raw,
                value_raw=value_raw,
                headers=raw["headers"],
                format=fmt,
            ))

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
) -> list[KafkaMessage]:
    """SQL mode: accumulate all messages in DuckDB then apply WHERE clause."""
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
            lambda: cancel_event.is_set(), batch_size=50000,
        ):
            if cancel_event.is_set():
                break

            rows = []
            for raw in batch:
                val, fmt = deserializer.deserialize(
                    raw["value_bytes"], raw["key_bytes"], request.format
                )
                key = raw["key_bytes"].decode("utf-8", errors="replace") if raw["key_bytes"] else None

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
        return [_row_to_message(row) for row in matched_rows]
    finally:
        con.close()


def _run_search(task_id: str) -> None:
    task = _tasks[task_id]
    cluster: ClusterConfig = task["cluster"]
    topic: str = task["topic"]
    request: SearchRequest = task["request"]
    cancel_event: threading.Event = task["cancel"]

    all_matched: list[KafkaMessage] = []

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
                    all_matched.sort(key=lambda m: (m.timestamp, m.partition, m.offset))
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


def _row_to_message(row: tuple) -> KafkaMessage:
    partition, msg_offset, ts, key, value_json, value_raw, headers_str, fmt = row
    parsed_val = json.loads(value_json) if value_json else value_raw
    return KafkaMessage(
        partition=partition,
        offset=msg_offset,
        timestamp=ts,
        timestamp_human=_ts_to_human(ts),
        key=key,
        value=parsed_val,
        value_raw=value_raw,
        headers=json.loads(headers_str) if headers_str else {},
        format=fmt,
    )


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


async def stream_results(task_id: str) -> AsyncGenerator[str, None]:
    task = _tasks.get(task_id)
    if not task:
        yield f"event: error\ndata: {json.dumps({'error': 'Task not found'})}\n\n"
        return

    last_matched = -1
    last_scanned = -1

    while True:
        with task["lock"]:
            scanned = task["scanned"]
            matched = task["matched"]
            done = task["done"]
            error = task["error"]
            results = list(task["results"])

        if scanned != last_scanned or matched != last_matched:
            progress = {"scanned": scanned, "matched": matched}
            yield f"event: progress\ndata: {orjson.dumps(progress).decode()}\n\n"
            last_scanned = scanned

        if matched != last_matched and matched > 0:
            msgs_data = [m.model_dump() for m in results]
            yield f"event: results\ndata: {orjson.dumps(msgs_data, option=orjson.OPT_NON_STR_KEYS).decode()}\n\n"
            last_matched = matched

        if error:
            yield f"event: error\ndata: {orjson.dumps({'error': error}).decode()}\n\n"
            break

        if done:
            final = [m.model_dump() for m in results]
            yield f"event: done\ndata: {orjson.dumps({'scanned': scanned, 'matched': matched, 'messages': final}, option=orjson.OPT_NON_STR_KEYS).decode()}\n\n"
            break

        changed = scanned != last_scanned or matched != last_matched
        await asyncio.sleep(0.05 if not changed else 0.0)
