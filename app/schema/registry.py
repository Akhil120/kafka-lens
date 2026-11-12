from __future__ import annotations
import httpx


class SchemaRegistryClient:
    def __init__(self, url: str, username: str | None = None, password: str | None = None):
        if url and "://" not in url:
            url = "http://" + url
        self.url = url.rstrip("/")
        self._auth = (username, password) if username else None

    def _get(self, path: str) -> dict:
        resp = httpx.get(
            f"{self.url}{path}",
            auth=self._auth,
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    def list_subjects(self) -> list[str]:
        return self._get("/subjects")

    def get_schema_by_id(self, schema_id: int) -> dict:
        return self._get(f"/schemas/ids/{schema_id}")

    def get_latest_schema(self, subject: str) -> dict:
        return self._get(f"/subjects/{subject}/versions/latest")

    def get_schema_version(self, subject: str, version: int) -> dict:
        return self._get(f"/subjects/{subject}/versions/{version}")
