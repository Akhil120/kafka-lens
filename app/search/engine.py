from __future__ import annotations
import asyncio
import json
import threading
import uuid
from datetime import datetime, timezone
from typing import AsyncGenerator, Any

import duckdb

from app.models import ClusterConfig, SearchRequest, KafkaMessage
from app.kafka.consumer import fetch_messages
from app.schema.deserializer import make_deserializer

_tasks: dict[str, dict] = {}


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


def _run_search(task_id: str) -> None:
    task = _tasks[task_id]
    cluster: ClusterConfig = task["cluster"]
    topic: str = task["topic"]
    request: SearchRequest = task["request"]
    cancel_event: threading.Event = task["cancel"]

    deserializer = make_deserializer(
        cluster.schema_registry_url,
        cluster.schema_registry_username,
        cluster.schema_registry_password,
        request.schema_registry_url,
    )

    all_matched: list[KafkaMessage] = []
    total_scanned = 0
    con = None

    try:
        con = duckdb.connect(":memory:")
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

        where_clauses, _ = _build_where(request)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        for batch in fetch_messages(cluster, topic, request, lambda: cancel_event.is_set(), batch_size=5000):
            if cancel_event.is_set():
                break

            rows = []
            for raw in batch:
                val, fmt = deserializer.deserialize(
                    raw["value_bytes"], raw["key_bytes"], request.format
                )
                key = raw["key_bytes"].decode("utf-8", errors="replace") if raw["key_bytes"] else None

                if isinstance(val, (dict, list)):
                    value_json = json.dumps(val, default=str)
                    value_raw = value_json
                else:
                    value_raw = str(val) if val is not None else ""
                    value_json = None

                rows.append((
                    raw["partition"],
                    raw["offset"],
                    raw["timestamp"],
                    key,
                    value_json,
                    value_raw,
                    json.dumps(raw["headers"]),
                    fmt,
                ))

            if rows:
                con.executemany(
                    "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows
                )
                total_scanned += len(rows)

                _, params = _build_where(request)
                matched_rows = con.execute(
                    f"SELECT * FROM messages {where_sql} ORDER BY timestamp, partition, msg_offset",
                    params,
                ).fetchall()

                new_messages = [_row_to_message(row) for row in matched_rows]
                all_matched.extend(new_messages)

                with task["lock"]:
                    task["results"] = list(all_matched)
                    task["matched"] = len(all_matched)
                    task["scanned"] = total_scanned

                con.execute("DELETE FROM messages")

    except Exception as e:
        with task["lock"]:
            task["error"] = str(e)
    finally:
        if con:
            con.close()
        with task["lock"]:
            task["scanned"] = total_scanned
            task["done"] = True


def _row_to_message(row: tuple) -> KafkaMessage:
    partition, msg_offset, ts, key, value_json, value_raw, headers_str, fmt = row
    offset = msg_offset
    parsed_val = json.loads(value_json) if value_json else value_raw
    return KafkaMessage(
        partition=partition,
        offset=offset,
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
        if vm == "jsonpath":
            path, _, expected = vf.partition("=")
            path = path.strip()
            expected = expected.strip().strip("\"'")
            if expected:
                clauses.append(f"json_extract_string(value_json, '{path}') = ?")
                params.append(expected)
            else:
                clauses.append(f"json_extract_string(value_json, '{path}') IS NOT NULL")
        elif vm == "sql":
            clauses.append(f"({vf})")
        else:
            clauses.append("value_raw ILIKE ?")
            params.append(f"%{vf}%")

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
            yield f"event: progress\ndata: {json.dumps(progress)}\n\n"
            last_scanned = scanned

        if matched != last_matched and matched > 0:
            msgs_data = [m.model_dump() for m in results]
            yield f"event: results\ndata: {json.dumps(msgs_data, default=str)}\n\n"
            last_matched = matched

        if error:
            yield f"event: error\ndata: {json.dumps({'error': error})}\n\n"
            break

        if done:
            final = [m.model_dump() for m in results]
            yield f"event: done\ndata: {json.dumps({'scanned': scanned, 'matched': matched, 'messages': final}, default=str)}\n\n"
            break

        await asyncio.sleep(0.3)
