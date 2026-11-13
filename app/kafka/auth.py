from __future__ import annotations
from app.models import ClusterConfig


def build_kafka_config(cluster: ClusterConfig, extra: dict | None = None) -> dict:
    cfg: dict = {
        "bootstrap.servers": cluster.bootstrap_servers,
        "socket.timeout.ms": 30000,
    }

    proto = cluster.security_protocol
    mech = cluster.sasl_mechanism

    cfg["security.protocol"] = proto

    if proto in ("SSL", "SASL_SSL"):
        from app.kafka.ssl import get_system_ca_bundle
        ssl = cluster.ssl

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
            cfg["ssl.endpoint.identification.algorithm"] = (
                ssl.endpoint_identification_algorithm
            )

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
