"""Group-free message fetcher using kafka-python-ng.

kafka-python-ng with group_id=None sends only Metadata + Fetch + ListOffsets
requests — no FindCoordinator, JoinGroup, or Heartbeat. No consumer group is
created so no Group ACL is required and nothing appears in Confluent group listings.
"""
from __future__ import annotations
import logging
import ssl as _ssl
import time
from typing import Generator, Callable

from app.models import ClusterConfig

log = logging.getLogger(__name__)


def _build_config(cluster: ClusterConfig) -> dict:
    cfg: dict = {
        "bootstrap_servers": cluster.bootstrap_servers,
        "client_id": "kafka-lens",
        "group_id": None,
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
        "request_timeout_ms": 30000,
        "api_version_auto_timeout_ms": 10000,
        "connections_max_idle_ms": 60000,
        "fetch_max_bytes": 52428800,
        "max_partition_fetch_bytes": 10485760,
    }

    proto = cluster.security_protocol
    mech = cluster.sasl_mechanism

    if proto in ("SSL", "SASL_SSL"):
        cfg["security_protocol"] = proto

        # Always build an explicit SSL context — avoids kafka-python-ng relying
        # on its internal OpenSSL defaults which may not include corporate CAs.
        ctx = _ssl.create_default_context()

        if cluster.tls_skip_verify:
            ctx.check_hostname = False
            ctx.verify_mode = _ssl.CERT_NONE

        if cluster.ssl:
            if cluster.ssl.ca_location:
                ctx.load_verify_locations(cluster.ssl.ca_location)
            if cluster.ssl.certificate_location and cluster.ssl.key_location:
                ctx.load_cert_chain(
                    cluster.ssl.certificate_location,
                    cluster.ssl.key_location,
                    cluster.ssl.key_password,
                )

        cfg["ssl_context"] = ctx

    elif proto == "SASL_PLAINTEXT":
        cfg["security_protocol"] = "SASL_PLAINTEXT"
    else:
        cfg["security_protocol"] = "PLAINTEXT"

    if proto.startswith("SASL") and mech in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
        cfg["sasl_mechanism"] = mech
        cfg["sasl_plain_username"] = cluster.username or ""
        cfg["sasl_plain_password"] = cluster.password or ""

    if proto.startswith("SASL") and mech in ("OAUTHBEARER", "AWS_MSK_IAM"):
        raise NotImplementedError(
            f"sasl_mechanism={mech} is not supported by the group-free fetcher."
        )

    return cfg


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
    """Yield batches of raw message dicts with no consumer group involvement."""
    try:
        from kafka import KafkaConsumer, TopicPartition as KTP
    except ImportError:
        raise RuntimeError(
            "kafka-python-ng is not installed. Run: pip install kafka-python-ng"
        )

    cfg = _build_config(cluster)
    log.info("fetcher: p%d [%d, %d) max=%d cfg=%s",
             pid, start_offset, end_offset, max_messages,
             {k: ("***" if "password" in k or "secret" in k else v) for k, v in cfg.items()})

    consumer = KafkaConsumer(**cfg)
    try:
        tp = KTP(topic, pid)
        consumer.assign([tp])

        # kafka-python requires at least one poll() to fetch partition metadata
        # before seek() takes effect. Do a zero-timeout poll first.
        consumer.poll(timeout_ms=0)
        consumer.seek(tp, start_offset)
        log.info("fetcher: p%d seeked to %d after initial metadata poll", pid, start_offset)

        batch: list[dict] = []
        total_fetched = 0
        consecutive_empty = 0
        last_progress = time.time()

        while total_fetched < max_messages:
            if cancelled():
                break
            if time.time() - last_progress > idle_timeout:
                log.warning("fetcher: p%d idle timeout after %.0fs, fetched %d msgs",
                            pid, idle_timeout, total_fetched)
                break

            records = consumer.poll(
                timeout_ms=1000,
                max_records=min(batch_size, max_messages - total_fetched),
            )

            if not records:
                consecutive_empty += 1
                if consecutive_empty % 10 == 0:
                    log.debug("fetcher: p%d %d consecutive empty polls", pid, consecutive_empty)
                continue

            consecutive_empty = 0
            last_progress = time.time()
            reached_end = False

            for msgs in records.values():
                for msg in msgs:
                    if msg.offset >= end_offset:
                        reached_end = True
                        break
                    batch.append({
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "timestamp": msg.timestamp,
                        "key_bytes": msg.key,
                        "value_bytes": msg.value,
                        "headers": {
                            k: v.decode("utf-8", errors="replace")
                            for k, v in (msg.headers or [])
                        },
                    })
                    total_fetched += 1
                if reached_end:
                    break

            log.debug("fetcher: p%d poll returned %d msgs, total=%d",
                      pid, sum(len(v) for v in records.values()), total_fetched)

            if len(batch) >= batch_size or reached_end:
                if batch:
                    yield batch
                    batch = []
                if reached_end:
                    break

        if batch:
            yield batch

        log.info("fetcher: p%d done, fetched %d msgs", pid, total_fetched)

    finally:
        consumer.close()
