import json
from typing import Any
from app.config import settings
from app.crypto import encrypt, decrypt
from app.models import ClusterConfig

_ENCRYPTED_FIELDS = ["password", "schema_registry_password"]


def _encrypt_cluster(data: dict) -> dict:
    out = dict(data)
    for field in _ENCRYPTED_FIELDS:
        if out.get(field):
            out[field] = encrypt(out[field])
    if out.get("oauth") and out["oauth"].get("client_secret"):
        out["oauth"] = dict(out["oauth"])
        out["oauth"]["client_secret"] = encrypt(out["oauth"]["client_secret"])
    return out


def _decrypt_cluster(data: dict) -> dict:
    out = dict(data)
    for field in _ENCRYPTED_FIELDS:
        if out.get(field):
            out[field] = decrypt(out[field])
    if out.get("oauth") and out["oauth"].get("client_secret"):
        out["oauth"] = dict(out["oauth"])
        out["oauth"]["client_secret"] = decrypt(out["oauth"]["client_secret"])
    return out


def _load_raw() -> list[dict]:
    cf = settings.clusters_file
    if not cf.exists():
        return []
    try:
        return json.loads(cf.read_text())
    except Exception:
        return []


def _save_raw(clusters: list[dict]) -> None:
    settings.clusters_file.write_text(json.dumps(clusters, indent=2, default=str))


def list_clusters() -> list[ClusterConfig]:
    return [ClusterConfig(**_decrypt_cluster(c)) for c in _load_raw()]


def get_cluster(cluster_id: str) -> ClusterConfig | None:
    for c in _load_raw():
        if c.get("id") == cluster_id:
            return ClusterConfig(**_decrypt_cluster(c))
    return None


def save_cluster(cluster: ClusterConfig) -> None:
    clusters = _load_raw()
    data = _encrypt_cluster(cluster.model_dump())
    for i, c in enumerate(clusters):
        if c.get("id") == cluster.id:
            clusters[i] = data
            _save_raw(clusters)
            return
    clusters.append(data)
    _save_raw(clusters)


def delete_cluster(cluster_id: str) -> bool:
    clusters = _load_raw()
    filtered = [c for c in clusters if c.get("id") != cluster_id]
    if len(filtered) == len(clusters):
        return False
    _save_raw(filtered)
    return True
