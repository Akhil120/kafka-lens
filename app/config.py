from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "info"
    data_dir: Path = Path.home() / ".kafka-lens"

    @property
    def key_file(self) -> Path:
        return self.data_dir / "key"

    @property
    def clusters_file(self) -> Path:
        return self.data_dir / "clusters.json"


settings = Settings()
settings.data_dir.mkdir(parents=True, exist_ok=True)
