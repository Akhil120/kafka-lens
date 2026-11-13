from __future__ import annotations
import atexit
import os
import platform
import subprocess
import tempfile

_ca_bundle_path: str | None = None
_resolved = False


def get_system_ca_bundle() -> str | None:
    """Return path to a PEM file containing all system-trusted CAs.

    Covers the case where a corporate SSL inspection proxy re-signs
    traffic with an org CA that librdkafka won't find on its own
    (librdkafka on macOS ignores the Keychain; browsers don't).
    Result is cached for the process lifetime.
    """
    global _ca_bundle_path, _resolved
    if _resolved:
        return _ca_bundle_path
    _resolved = True

    system = platform.system()
    if system == "Darwin":
        _ca_bundle_path = _macos_ca_bundle()
    elif system == "Windows":
        _ca_bundle_path = _windows_ca_bundle()
    else:
        _ca_bundle_path = _linux_ca_bundle()

    return _ca_bundle_path


def _macos_ca_bundle() -> str | None:
    # System.keychain holds org CAs pushed via MDM/JAMF.
    # SystemRootCertificates holds Apple's public root bundle.
    keychains = [
        "/Library/Keychains/System.keychain",
        "/System/Library/Keychains/SystemRootCertificates.keychain",
    ]
    blocks: list[str] = []
    for kc in keychains:
        try:
            r = subprocess.run(
                ["security", "find-certificate", "-a", "-p", kc],
                capture_output=True,
                timeout=10,
            )
            if r.returncode == 0 and r.stdout:
                blocks.append(r.stdout.decode("utf-8", errors="replace"))
        except Exception:
            pass
    return _write_pem("\n".join(blocks)) if blocks else None

def _windows_ca_bundle() -> str | None:
    # librdkafka on Windows uses OpenSSL, which does NOT automatically read the
    # Windows cert store. certifi ships Mozilla's root CA bundle and covers all
    # standard public CAs including those used by Confluent Cloud.
    try:
        import certifi
        return certifi.where()
    except ImportError:
        return None


def _linux_ca_bundle() -> str | None:
    for path in (
        "/etc/ssl/certs/ca-certificates.crt",  # Debian / Ubuntu / Alpine
        "/etc/pki/tls/certs/ca-bundle.crt",    # RHEL / CentOS / Fedora
        "/etc/ssl/ca-bundle.pem",              # openSUSE
        "/etc/ssl/cert.pem",                   # FreeBSD / some Alpine
    ):
        if os.path.isfile(path):
            return path
    return None


def _write_pem(content: str) -> str | None:
    try:
        fd, path = tempfile.mkstemp(prefix="kafka_lens_ca_", suffix=".pem")
        with os.fdopen(fd, "w") as f:
            f.write(content)
        atexit.register(_unlink, path)
        return path
    except Exception:
        return None


def _unlink(path: str) -> None:
    try:
        os.unlink(path)
    except Exception:
        pass
