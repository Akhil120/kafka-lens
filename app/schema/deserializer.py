from __future__ import annotations
import io
import json
import struct
from functools import lru_cache
from typing import Any

import fastavro

from app.schema.registry import SchemaRegistryClient

MAGIC_BYTE = 0x00


def _is_confluent_encoded(data: bytes) -> bool:
    return len(data) > 5 and data[0] == MAGIC_BYTE


def _extract_schema_id(data: bytes) -> tuple[int, bytes]:
    schema_id = struct.unpack(">I", data[1:5])[0]
    return schema_id, data[5:]


class MessageDeserializer:
    def __init__(self, registry: SchemaRegistryClient | None = None):
        self._registry = registry
        self._schema_cache: dict[int, Any] = {}

    def _get_avro_schema(self, schema_id: int):
        if schema_id not in self._schema_cache:
            meta = self._registry.get_schema_by_id(schema_id)
            parsed = fastavro.parse_schema(json.loads(meta["schema"]))
            self._schema_cache[schema_id] = parsed
        return self._schema_cache[schema_id]

    def _get_proto_descriptor(self, schema_id: int):
        from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
        if schema_id not in self._schema_cache:
            meta = self._registry.get_schema_by_id(schema_id)
            self._schema_cache[schema_id] = meta["schema"]
        return self._schema_cache[schema_id]

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
                # peek at registry to determine type — default to avro
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
            if detected_fmt in ("avro", "protobuf", "json") and _is_confluent_encoded(value_bytes):
                schema_id, _ = _extract_schema_id(value_bytes)
                return {
                    "_error": str(e),
                    "_hint": f"Could not decode {detected_fmt} message (schema_id={schema_id}). Check schema registry configuration.",
                    "_raw_bytes": len(value_bytes),
                }, "error"
            return value_bytes.decode("utf-8", errors="replace"), "plain"

    def _decode_avro(self, data: bytes) -> Any:
        if _is_confluent_encoded(data):
            schema_id, payload = _extract_schema_id(data)
            schema = self._get_avro_schema(schema_id)
            reader = io.BytesIO(payload)
            return fastavro.schemaless_reader(reader, schema)
        # try raw avro with embedded schema
        reader = io.BytesIO(data)
        records = list(fastavro.reader(reader))
        return records[0] if len(records) == 1 else records

    def _decode_protobuf(self, data: bytes) -> Any:
        if _is_confluent_encoded(data):
            schema_id, payload = _extract_schema_id(data)
            # skip message index bytes (Confluent protobuf framing)
            # first byte(s) are zigzag-encoded message indexes
            idx = 0
            while idx < len(payload) and payload[idx] & 0x80:
                idx += 1
            idx += 1
            payload = payload[idx:]
            # generic decode as unknown fields — return hex if no descriptor
            return {"_raw_hex": payload.hex(), "_note": "protobuf (install descriptor for full decode)"}
        return {"_raw_hex": data.hex()}

    def _decode_json(self, data: bytes) -> Any:
        if _is_confluent_encoded(data):
            _, payload = _extract_schema_id(data)
        else:
            payload = data
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
