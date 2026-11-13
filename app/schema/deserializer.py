from __future__ import annotations
import io
import json
import logging
import struct
from typing import Any

import fastavro

from app.schema.registry import SchemaRegistryClient

log = logging.getLogger(__name__)

MAGIC_BYTE = 0x00

# Module-level cache shared across all deserialization calls so we hit the
# registry only once per schema_id regardless of how many partitions run in
# parallel.
_schema_cache: dict[tuple[str, int], Any] = {}   # (registry_url, schema_id) -> parsed schema
_schema_type_cache: dict[tuple[str, int], str] = {}  # (registry_url, schema_id) -> "avro"|"json"|"protobuf"


def _is_confluent_encoded(data: bytes) -> bool:
    return len(data) > 5 and data[0] == MAGIC_BYTE


def _extract_schema_id(data: bytes) -> tuple[int, bytes]:
    schema_id = struct.unpack(">I", data[1:5])[0]
    return schema_id, data[5:]


class MessageDeserializer:
    def __init__(self, registry: SchemaRegistryClient | None = None):
        self._registry = registry

    def _registry_key(self, schema_id: int) -> tuple[str, int]:
        return (self._registry.url if self._registry else "", schema_id)

    def _get_schema_type(self, schema_id: int) -> str:
        """Return 'avro', 'json', or 'protobuf' based on the registry schemaType field."""
        key = self._registry_key(schema_id)
        if key not in _schema_type_cache:
            meta = self._registry.get_schema_by_id(schema_id)
            raw_type = meta.get("schemaType", "AVRO").upper()
            _schema_type_cache[key] = {"AVRO": "avro", "JSON": "json", "PROTOBUF": "protobuf"}.get(raw_type, "avro")
        return _schema_type_cache[key]

    def _get_avro_schema(self, schema_id: int) -> Any:
        key = self._registry_key(schema_id)
        if key not in _schema_cache:
            meta = self._registry.get_schema_by_id(schema_id)
            raw = json.loads(meta["schema"])
            _schema_cache[key] = fastavro.parse_schema(raw)
        return _schema_cache[key]

    def deserialize(
        self,
        value_bytes: bytes | None,
        key_bytes: bytes | None,
        fmt: str,
    ) -> tuple[Any, str]:
        if value_bytes is None:
            return None, "plain"

        detected_fmt = fmt
        if fmt == "auto":
            if _is_confluent_encoded(value_bytes):
                if self._registry:
                    try:
                        schema_id, _ = _extract_schema_id(value_bytes)
                        detected_fmt = self._get_schema_type(schema_id)
                    except Exception as e:
                        log.warning("deserializer: could not determine schema type from registry: %s", e)
                        detected_fmt = "avro"
                else:
                    detected_fmt = "avro"
            else:
                detected_fmt = _sniff_format(value_bytes)

        try:
            if detected_fmt == "avro":
                result = self._decode_avro(value_bytes)
            elif detected_fmt == "protobuf":
                result = self._decode_protobuf(value_bytes)
            elif detected_fmt == "json":
                result = self._decode_json(value_bytes)
            else:
                result = value_bytes.decode("utf-8", errors="replace")
            return result, detected_fmt
        except Exception as e:
            log.warning("deserializer: failed to decode %s message: %s", detected_fmt, e)
            if _is_confluent_encoded(value_bytes):
                schema_id, _ = _extract_schema_id(value_bytes)
                hint = (
                    f"Could not decode {detected_fmt} message (schema_id={schema_id})."
                    if self._registry else
                    f"Message has Confluent magic byte (schema_id={schema_id}) but no schema registry is configured."
                )
                return {"_error": str(e), "_hint": hint, "_raw_bytes": len(value_bytes)}, "error"
            return value_bytes.decode("utf-8", errors="replace"), "plain"

    def _decode_avro(self, data: bytes) -> Any:
        if not self._registry and _is_confluent_encoded(data):
            raise ValueError("Schema registry not configured — cannot decode Confluent-encoded Avro")
        if _is_confluent_encoded(data):
            schema_id, payload = _extract_schema_id(data)
            schema = self._get_avro_schema(schema_id)
            return fastavro.schemaless_reader(io.BytesIO(payload), schema)
        # raw Avro container file with embedded schema
        records = list(fastavro.reader(io.BytesIO(data)))
        return records[0] if len(records) == 1 else records

    def _decode_protobuf(self, data: bytes) -> Any:
        if _is_confluent_encoded(data):
            schema_id, payload = _extract_schema_id(data)
            # skip Confluent message-index varint prefix
            idx = 0
            while idx < len(payload) and payload[idx] & 0x80:
                idx += 1
            payload = payload[idx + 1:]
            return {"_raw_hex": payload.hex(), "_note": "protobuf (install descriptor for full decode)"}
        return {"_raw_hex": data.hex()}

    def _decode_json(self, data: bytes) -> Any:
        payload = data[5:] if _is_confluent_encoded(data) else data
        return json.loads(payload.decode("utf-8"))


def _sniff_format(data: bytes) -> str:
    stripped = data.lstrip()
    if stripped and stripped[0] in (ord("{"), ord("[")):
        try:
            json.loads(data.decode("utf-8"))
            return "json"
        except Exception:
            pass
    return "plain"


def make_deserializer(
    cluster_registry_url: str | None,
    cluster_registry_user: str | None,
    cluster_registry_pass: str | None,
    override_url: str | None = None,
) -> MessageDeserializer:
    url = override_url or cluster_registry_url
    if url:
        registry = SchemaRegistryClient(url, cluster_registry_user, cluster_registry_pass)
    else:
        registry = None
    return MessageDeserializer(registry)
