from pathlib import Path
from cryptography.fernet import Fernet
from app.config import settings


def _load_or_create_key() -> bytes:
    kf = settings.key_file
    if kf.exists():
        return kf.read_bytes()
    key = Fernet.generate_key()
    kf.write_bytes(key)
    kf.chmod(0o600)
    return key


_fernet = Fernet(_load_or_create_key())


def encrypt(plaintext: str | None) -> str | None:
    if plaintext is None:
        return None
    return _fernet.encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str | None) -> str | None:
    if ciphertext is None:
        return None
    try:
        return _fernet.decrypt(ciphertext.encode()).decode()
    except Exception:
        return ciphertext  # already plaintext (legacy)
