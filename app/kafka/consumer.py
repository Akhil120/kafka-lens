from __future__ import annotations
import time
from datetime import datetime
from typing import Generator, Callable
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING, OFFSET_END
from confluent_kafka.admin import AdminClient, NewTopic
from app.models import ClusterConfig, TopicInfo
from app.kafka.auth import build_kafka_config


def make_admin(cluster: ClusterConfig) -> AdminClient:
    cfg = build_kafka_config(cluster)
    return AdminClient(cfg)


def list_topics(cluster: ClusterConfig) -> list[dict]:
    admin = make_admin(cluster)
    meta = admin.list_topics(timeout=10)
    topics = []
    for name, topic_meta in sorted(meta.topics.items()):
        if name.startswith("_"):
            continue
        topics.append({
            "name": name,
            "partitions": len(topic_meta.partitions),
        })
    return topics


def get_topic_info(cluster: ClusterConfig, topic: str) -> TopicInfo:
    admin = make_admin(cluster)
    meta = admin.list_topics(topic=topic, timeout=10)
    topic_meta = meta.topics.get(topic)
    if not topic_meta:
        raise ValueError(f"Topic '{topic}' not found")

    partition_ids = list(topic_meta.partitions.keys())
    replication = max(
        len(p.replicas) for p in topic_meta.partitions.values()
    ) if topic_meta.partitions else 1

    cfg = build_kafka_config(cluster, {"group.id": "__kafka_lens_meta__", "enable.auto.commit": "false"})
    consumer = Consumer(cfg)

    tps_low = [TopicPartition(topic, pid, OFFSET_BEGINNING) for pid in partition_ids]
    tps_high = [TopicPartition(topic, pid, OFFSET_END) for pid in partition_ids]

    low_wm: dict[int, int] = {}
    high_wm: dict[int, int] = {}

    for pid in partition_ids:
        lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=5)
        low_wm[pid] = lo
        high_wm[pid] = hi

    consumer.close()
    total = sum(high_wm[p] - low_wm[p] for p in partition_ids)

    return TopicInfo(
        name=topic,
        partitions=len(partition_ids),
        replication_factor=replication,
        total_messages=total,
        low_watermarks=low_wm,
        high_watermarks=high_wm,
    )


def test_connection(cluster: ClusterConfig) -> list[dict]:
    admin = make_admin(cluster)
    meta = admin.list_topics(timeout=10)
    brokers = [
        {"id": b.id, "host": b.host, "port": b.port}
        for b in meta.brokers.values()
    ]
    return brokers


def _resolve_offsets_from_timestamps(
    consumer: Consumer,
    topic: str,
    partition_ids: list[int],
    from_ts: datetime | None,
    to_ts: datetime | None,
    high_wm: dict[int, int],
) -> tuple[dict[int, int], dict[int, int]]:
    start_offsets: dict[int, int] = {}
    end_offsets: dict[int, int] = {}

    if from_ts:
        from_ms = int(from_ts.timestamp() * 1000)
        tps = [TopicPartition(topic, pid, from_ms) for pid in partition_ids]
        resolved = consumer.offsets_for_times(tps, timeout=10)
        for tp in resolved:
            start_offsets[tp.partition] = tp.offset if tp.offset >= 0 else high_wm.get(tp.partition, 0)
    else:
        for pid in partition_ids:
            start_offsets[pid] = 0

    if to_ts:
        to_ms = int(to_ts.timestamp() * 1000)
        tps = [TopicPartition(topic, pid, to_ms) for pid in partition_ids]
        resolved = consumer.offsets_for_times(tps, timeout=10)
        for tp in resolved:
            end_offsets[tp.partition] = tp.offset if tp.offset >= 0 else high_wm.get(tp.partition, 0)
    else:
        end_offsets = dict(high_wm)

    return start_offsets, end_offsets


def fetch_messages(
    cluster: ClusterConfig,
    topic: str,
    request,
    cancelled: Callable[[], bool],
    batch_size: int = 500,
) -> Generator[list[dict], None, None]:
    cfg = build_kafka_config(cluster, {
        "group.id": "__kafka_lens_search__",
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest",
        "fetch.max.bytes": 52428800,
        "max.partition.fetch.bytes": 10485760,
    })
    consumer = Consumer(cfg)

    try:
        admin = make_admin(cluster)
        meta = admin.list_topics(topic=topic, timeout=10)
        topic_meta = meta.topics.get(topic)
        if not topic_meta:
            raise ValueError(f"Topic '{topic}' not found")

        all_partitions = list(topic_meta.partitions.keys())
        target_partitions = request.partitions if request.partitions else all_partitions

        high_wm: dict[int, int] = {}
        low_wm: dict[int, int] = {}
        for pid in target_partitions:
            lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=5)
            low_wm[pid] = lo
            high_wm[pid] = hi

        if request.from_timestamp or request.to_timestamp:
            start_offsets, end_offsets = _resolve_offsets_from_timestamps(
                consumer, topic, target_partitions,
                request.from_timestamp, request.to_timestamp, high_wm
            )
        else:
            start_offsets = {pid: (request.from_offset or low_wm[pid]) for pid in target_partitions}
            end_offsets = {pid: (request.to_offset or high_wm[pid]) for pid in target_partitions}

        assignments = []
        for pid in target_partitions:
            start = max(start_offsets.get(pid, 0), low_wm.get(pid, 0))
            end = min(end_offsets.get(pid, high_wm.get(pid, 0)), high_wm.get(pid, 0))
            if start < end:
                tp = TopicPartition(topic, pid, start)
                assignments.append((tp, end))

        if not assignments:
            return

        consumer.assign([a[0] for a in assignments])
        end_map = {a[0].partition: a[1] for a in assignments}

        exhausted = set()
        batch = []
        total_fetched = 0
        max_messages = request.max_messages
        assignment_tps = [a[0] for a in assignments]
        consecutive_empty = 0

        while len(exhausted) < len(assignments) and total_fetched < max_messages:
            if cancelled():
                break

            want = min(batch_size, max_messages - total_fetched)
            msgs = consumer.consume(num_messages=want, timeout=1.0)

            if not msgs:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    pos = consumer.position(assignment_tps)
                    for tp in pos:
                        if tp.partition not in exhausted and tp.offset >= end_map.get(tp.partition, 0):
                            exhausted.add(tp.partition)
                    if len(exhausted) >= len(assignments):
                        break
                continue

            consecutive_empty = 0
            for msg in msgs:
                if msg.error():
                    continue

                pid = msg.partition()
                if msg.offset() >= end_map.get(pid, 0):
                    exhausted.add(pid)
                    continue

                _, ts_val = msg.timestamp()
                headers_raw = msg.headers() or []
                batch.append({
                    "partition": pid,
                    "offset": msg.offset(),
                    "timestamp": ts_val,
                    "key_bytes": msg.key(),
                    "value_bytes": msg.value(),
                    "headers": {k: v.decode("utf-8", errors="replace") for k, v in headers_raw},
                })
                total_fetched += 1

            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    finally:
        consumer.close()
