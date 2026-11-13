from __future__ import annotations
import collections
import logging
import sys
from app.models import ClusterConfig

# Buffer the last 100 librdkafka log lines so test_connection can include them
# in error messages when SSL/broker failures occur.
_rdkafka_log_buffer: collections.deque[str] = collections.deque(maxlen=100)

_fmt = logging.Formatter("%(levelname)s [librdkafka] %(message)s")


class _BufHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        _rdkafka_log_buffer.append(self.format(record))


# Use a dedicated logger with its own stderr handler so logs always appear in
# the terminal regardless of how uvicorn configures the root logger level.
_rdkafka_logger = logging.getLogger("librdkafka")
_rdkafka_logger.setLevel(logging.DEBUG)
_rdkafka_logger.propagate = False  # don't double-emit via root logger

_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.setFormatter(_fmt)
_rdkafka_logger.addHandler(_stderr_handler)

_buf_handler = _BufHandler()
_buf_handler.setFormatter(_fmt)
_rdkafka_logger.addHandler(_buf_handler)


def get_rdkafka_logs() -> list[str]:
    return list(_rdkafka_log_buffer)


def build_kafka_config(cluster: ClusterConfig, extra: dict | None = None) -> dict:
    cfg: dict = {
        "bootstrap.servers": cluster.bootstrap_servers,
        "socket.timeout.ms": 30000,
        "broker.address.family": "v4",
    }

    proto = cluster.security_protocol
    mech = cluster.sasl_mechanism

    cfg["security.protocol"] = proto

    if proto in ("SSL", "SASL_SSL"):
        import sys
        from app.kafka.ssl import get_system_ca_bundle
        ssl = cluster.ssl

        # On Windows, use the Windows certificate store (Root + Intermediate CA).
        # Setting ssl.ca.location overrides the Windows store entirely and certifi
        # only has root CAs — the intermediate CA store is needed to build full
        # TLS chains. ssl.ca.certificate.stores is Windows-only and ignored elsewhere.
        if sys.platform == "win32":
            cfg["ssl.ca.certificate.stores"] = "CA,Root"

        # CA cert: explicit override wins; otherwise inject system bundle
        # so org proxy CAs (pushed via GPO/MDM) are trusted by librdkafka.
        if ssl and ssl.ca_location:
            cfg["ssl.ca.location"] = ssl.ca_location
        else:
            bundle = get_system_ca_bundle()
            if bundle:
                cfg["ssl.ca.location"] = bundle

        if ssl:
            if ssl.certificate_location:
                cfg["ssl.certificate.location"] = ssl.certificate_location
            if ssl.key_location:
                cfg["ssl.key.location"] = ssl.key_location
            if ssl.key_password:
                cfg["ssl.key.password"] = ssl.key_password

        # For private routing / PrivateLink: broker certs may be signed by an
        # internal CA (not in certifi) and hostnames may not match the SAN.
        # Java works because JSSE uses the Windows cert store; librdkafka uses
        # OpenSSL with certifi which won't have the enterprise CA.
        if cluster.tls_skip_verify:
            cfg["ssl.endpoint.identification.algorithm"] = "none"
            cfg["enable.ssl.certificate.verification"] = "false"

    if proto in ("SASL_PLAINTEXT", "SASL_SSL"):
        if mech == "AWS_MSK_IAM":
            _apply_msk_iam(cfg)
        elif mech == "OAUTHBEARER":
            _apply_oauth(cfg, cluster)
        else:
            cfg["sasl.mechanism"] = mech
            cfg["sasl.username"] = cluster.username or ""
            cfg["sasl.password"] = cluster.password or ""

    if extra:
        cfg.update(extra)

    return cfg


def _apply_msk_iam(cfg: dict) -> None:
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        def token_provider(config):
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(
                config.get("aws_region", "us-east-1")
            )
            return token, expiry_ms

        cfg["sasl.mechanism"] = "OAUTHBEARER"
        cfg["oauth_cb"] = token_provider
    except ImportError:
        raise RuntimeError(
            "aws-msk-iam-sasl-signer-python is required for AWS MSK IAM auth. "
            "Install it with: pip install aws-msk-iam-sasl-signer-python"
        )


def _apply_oauth(cfg: dict, cluster: ClusterConfig) -> None:
    import httpx

    oauth = cluster.oauth
    if not oauth:
        raise ValueError("OAuth config is required for OAUTHBEARER mechanism")

    def token_provider(config):
        resp = httpx.post(
            oauth.token_endpoint_url,
            data={
                "grant_type": "client_credentials",
                "client_id": oauth.client_id,
                "client_secret": oauth.client_secret,
                **({"scope": oauth.scope} if oauth.scope else {}),
            },
            timeout=10,
        )
        resp.raise_for_status()
        body = resp.json()
        token = body["access_token"]
        expires_in = body.get("expires_in", 3600)
        import time
        expiry_ms = int((time.time() + expires_in) * 1000)
        return token, expiry_ms

    cfg["sasl.mechanism"] = "OAUTHBEARER"
    cfg["oauth_cb"] = token_provider
