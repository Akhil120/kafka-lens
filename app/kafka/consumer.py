from __future__ import annotations
import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Generator, Callable
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING, OFFSET_END
from confluent_kafka.admin import AdminClient, NewTopic
from app.models import ClusterConfig, TopicInfo
from app.kafka.auth import build_kafka_config, _rdkafka_logger  # noqa: F401 – logger used as Consumer kwarg

log = logging.getLogger(__name__)

_admin_cache: dict[str, AdminClient] = {}
_admin_lock = threading.Lock()


def make_admin(cluster: ClusterConfig) -> AdminClient:
    cfg = build_kafka_config(cluster)
    return AdminClient(cfg)  # AdminClient doesn't support logger kwarg


def get_admin(cluster: ClusterConfig) -> AdminClient:
    """Return a cached AdminClient for the cluster, creating one if needed."""
    cid = str(cluster.id)
    with _admin_lock:
        if cid not in _admin_cache:
            _admin_cache[cid] = make_admin(cluster)
        return _admin_cache[cid]


def invalidate_admin(cluster_id: str) -> None:
    with _admin_lock:
        _admin_cache.pop(str(cluster_id), None)


_REDACT = {"sasl.password", "ssl.key.password", "ssl.keystore.password"}


def _safe_cfg(cfg: dict) -> dict:
    return {k: ("***" if k in _REDACT else v) for k, v in cfg.items()}


def list_topics(cluster: ClusterConfig) -> list[dict]:
    from confluent_kafka import KafkaError

    log.info("list_topics: building config for %r (%s)", cluster.name, cluster.bootstrap_servers)
    cfg = build_kafka_config(cluster)
    log.info("list_topics: effective kafka config: %s", _safe_cfg(cfg))

    log.info("list_topics: creating AdminClient for %r", cluster.name)
    try:
        admin = AdminClient(cfg)
    except Exception as exc:
        log.error("list_topics: AdminClient() raised for %r: %s", cluster.name, exc, exc_info=True)
        raise

    log.info("list_topics: calling admin.list_topics(timeout=15) for %r", cluster.name)
    try:
        meta = admin.list_topics(timeout=15)
    except Exception as exc:
        log.error("list_topics: admin.list_topics() raised for %r: %s", cluster.name, exc, exc_info=True)
        raise

    broker_ids = list(meta.brokers.keys())
    log.info("list_topics: metadata served by broker %s; brokers in cluster: %s",
             meta.orig_broker_name, broker_ids)

    raw_names = list(meta.topics.keys())
    log.info("list_topics: Kafka returned %d raw topic entries for %r (first 20: %s)",
             len(raw_names), cluster.name, raw_names[:20])

    if not raw_names:
        log.warning(
            "list_topics: Kafka returned 0 topics for %r — likely causes: "
            "(1) API key lacks DESCRIBE ACL on topics, "
            "(2) TLS hostname check is blocking broker metadata connections "
            "(enable 'Skip TLS hostname verification' in cluster settings)",
            cluster.name,
        )
        raise ValueError(
            "Kafka returned 0 topics. Possible causes: "
            "(1) API key lacks DESCRIBE ACL on Topic:*, "
            "(2) TLS hostname mismatch on broker connections — enable "
            "'Skip TLS hostname verification' in the cluster settings."
        )

    topics = []
    skipped_internal = 0
    for name, topic_meta in sorted(meta.topics.items()):
        if topic_meta.error and topic_meta.error.code() != KafkaError.NO_ERROR:
            log.warning("list_topics: topic %r has metadata error: %s", name, topic_meta.error)
        if name.startswith("_"):
            skipped_internal += 1
            continue
        topics.append({
            "name": name,
            "partitions": len(topic_meta.partitions),
        })

    log.info("list_topics: returning %d user topics for %r (%d internal skipped)",
             len(topics), cluster.name, skipped_internal)
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
    consumer = Consumer(cfg, logger=_rdkafka_logger)

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
    import socket as _socket

    # TCP pre-check: separates "can't reach host" from "SSL/auth issue"
    for entry in cluster.bootstrap_servers.split(","):
        entry = entry.strip()
        host, _, port_str = entry.rpartition(":")
        host = host or entry
        port = int(port_str) if port_str.isdigit() else 9092
        try:
            s = _socket.create_connection((host, port), timeout=5)
            s.close()
        except OSError as e:
            raise RuntimeError(
                f"Cannot reach {host}:{port} over TCP — check firewall or VPN. ({e})"
            )

    try:
        admin = make_admin(cluster)
        meta = admin.list_topics(timeout=15)
        return [
            {"id": b.id, "host": b.host, "port": b.port}
            for b in meta.brokers.values()
        ]
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


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
    consumer = Consumer(cfg, logger=_rdkafka_logger)

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


def resolve_partition_ranges(
    cluster: ClusterConfig,
    topic: str,
    request,
) -> dict[int, tuple[int, int]]:
    """Return {pid: (start_offset, end_offset)} using AdminClient only (no Consumer)."""
    from confluent_kafka.admin import OffsetSpec

    admin = get_admin(cluster)
    meta = admin.list_topics(topic=topic, timeout=10)
    topic_meta = meta.topics.get(topic)
    if not topic_meta:
        raise ValueError(f"Topic '{topic}' not found")

    all_partitions = list(topic_meta.partitions.keys())
    target_partitions = request.partitions if request.partitions else all_partitions

    low_futures = admin.list_offsets(
        {TopicPartition(topic, pid): OffsetSpec.earliest() for pid in target_partitions},
        request_timeout=10,
    )
    high_futures = admin.list_offsets(
        {TopicPartition(topic, pid): OffsetSpec.latest() for pid in target_partitions},
        request_timeout=10,
    )
    low_wm = {tp.partition: f.result().offset for tp, f in low_futures.items()}
    high_wm = {tp.partition: f.result().offset for tp, f in high_futures.items()}

    if request.from_timestamp:
        from_ms = int(request.from_timestamp.timestamp() * 1000)
        ts_futures = admin.list_offsets(
            {TopicPartition(topic, pid): OffsetSpec.for_timestamp(from_ms) for pid in target_partitions},
            request_timeout=10,
        )
        start_offsets = {
            tp.partition: (f.result().offset if f.result().offset >= 0 else high_wm.get(tp.partition, 0))
            for tp, f in ts_futures.items()
        }
    else:
        start_offsets = {pid: (request.from_offset or low_wm[pid]) for pid in target_partitions}

    if request.to_timestamp:
        to_ms = int(request.to_timestamp.timestamp() * 1000)
        ts_futures = admin.list_offsets(
            {TopicPartition(topic, pid): OffsetSpec.for_timestamp(to_ms) for pid in target_partitions},
            request_timeout=10,
        )
        end_offsets = {
            tp.partition: (f.result().offset if f.result().offset >= 0 else high_wm.get(tp.partition, 0))
            for tp, f in ts_futures.items()
        }
    else:
        end_offsets = {pid: (request.to_offset or high_wm[pid]) for pid in target_partitions}

    result: dict[int, tuple[int, int]] = {}
    for pid in target_partitions:
        start = max(start_offsets.get(pid, 0), low_wm.get(pid, 0))
        end = min(end_offsets.get(pid, high_wm.get(pid, 0)), high_wm.get(pid, 0))
        if start < end:
            result[pid] = (start, end)
    return result


def fetch_partition(
    cluster: ClusterConfig,
    topic: str,
    pid: int,
    start_offset: int,
    end_offset: int,
    max_messages: int,
    cancelled: Callable[[], bool],
    batch_size: int = 50000,
    idle_timeout: float = 30.0,
) -> Generator[list[dict], None, None]:
    """Consume a single partition from start_offset up to end_offset, yielding batches."""
    cfg = build_kafka_config(cluster, {
        "group.id": f"__kafka_lens_{uuid.uuid4().hex}__",
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest",
        "fetch.max.bytes": 52428800,
        "max.partition.fetch.bytes": 10485760,
    })
    consumer = Consumer(cfg, logger=_rdkafka_logger)
    try:
        consumer.assign([TopicPartition(topic, pid, start_offset)])

        batch: list[dict] = []
        total_fetched = 0
        consecutive_empty = 0
        last_progress = time.time()

        while total_fetched < max_messages:
            if cancelled():
                break

            if time.time() - last_progress > idle_timeout:
                break

            want = min(batch_size, max_messages - total_fetched)
            msgs = consumer.consume(num_messages=want, timeout=1.0)

            if not msgs:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    pos = consumer.position([TopicPartition(topic, pid)])
                    if pos and pos[0].offset >= end_offset:
                        break
                continue

            consecutive_empty = 0
            last_progress = time.time()
            reached_end = False
            for msg in msgs:
                if msg.error():
                    continue
                if msg.offset() >= end_offset:
                    reached_end = True
                    break

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

            if len(batch) >= batch_size or reached_end:
                if batch:
                    yield batch
                    batch = []
                if reached_end:
                    break

        if batch:
            yield batch
    finally:
        consumer.close()
