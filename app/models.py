from __future__ import annotations
from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel, Field, model_validator
import uuid


class SslConfig(BaseModel):
    ca_location: str | None = None
    certificate_location: str | None = None
    key_location: str | None = None
    key_password: str | None = None
    endpoint_identification_algorithm: str = "none"


class OauthConfig(BaseModel):
    token_endpoint_url: str
    client_id: str
    client_secret: str
    scope: str | None = None


class ClusterConfig(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    bootstrap_servers: str

    security_protocol: Literal[
        "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"
    ] = "PLAINTEXT"

    sasl_mechanism: Literal[
        "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "AWS_MSK_IAM", ""
    ] = ""

    username: str | None = None
    password: str | None = None  # encrypted at rest

    ssl: SslConfig | None = None
    tls_skip_verify: bool = False  # sets ssl.endpoint.identification.algorithm=none
    oauth: OauthConfig | None = None  # password/client_secret encrypted at rest

    schema_registry_url: str | None = None
    schema_registry_username: str | None = None
    schema_registry_password: str | None = None  # encrypted at rest

    created_at: datetime = Field(default_factory=datetime.utcnow)

    @model_validator(mode="after")
    def validate_sasl(self) -> "ClusterConfig":
        if self.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
            if not self.sasl_mechanism:
                raise ValueError("sasl_mechanism is required when security_protocol is SASL")
            if self.sasl_mechanism in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
                if not self.username or not self.password:
                    raise ValueError(
                        f"username and password are required for sasl_mechanism={self.sasl_mechanism}"
                    )
        return self


class ClusterSummary(BaseModel):
    id: str
    name: str
    bootstrap_servers: str
    security_protocol: str
    sasl_mechanism: str
    schema_registry_url: str | None
    tls_skip_verify: bool = False


class TestConnectionResponse(BaseModel):
    ok: bool
    brokers: list[dict[str, Any]] = []
    error: str | None = None


class TopicInfo(BaseModel):
    name: str
    partitions: int
    replication_factor: int
    total_messages: int
    low_watermarks: dict[int, int]
    high_watermarks: dict[int, int]
    schema_subject: str | None = None


class SearchRequest(BaseModel):
    partitions: list[int] | None = None
    from_offset: int | None = None
    to_offset: int | None = None
    from_timestamp: datetime | None = None
    to_timestamp: datetime | None = None

    key_filter: str | None = None
    key_match: Literal["exact", "contains", "prefix", "regex"] = "contains"

    value_filter: str | None = None
    value_filter_mode: Literal["jsonpath", "fulltext", "sql"] = "fulltext"

    format: Literal["auto", "json", "avro", "protobuf", "plain"] = "auto"
    schema_registry_url: str | None = None

    max_messages: int = Field(default=1000, ge=1)


class KafkaMessage(BaseModel):
    partition: int
    offset: int
    timestamp: int
    timestamp_human: str
    key: str | None
    value: Any
    value_raw: str
    headers: dict[str, str]
    format: str


class SearchTaskResponse(BaseModel):
    task_id: str
