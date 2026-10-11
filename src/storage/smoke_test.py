"""Collaudo di upload, download e integrità sui due sistemi di storage.

Eseguire dal container Jupyter (CLI o notebook); crea un file locale da 10 KiB.
"""
from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

from .hdfs_client import HDFSClient
from .ozone_client import OzoneClient


def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    """Calcola l'hash SHA-256 usando memoria costante."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        for piece in iter(lambda: file.read(chunk_size), b""):
            digest.update(piece)
    return digest.hexdigest()


def run_smoke_test() -> dict[str, str]:
    """Collauda entrambi i sistemi e ritorna gli hash verificati."""
    results: dict[str, str] = {}
    with tempfile.TemporaryDirectory(prefix="asdbd-smoke-") as temp:
        directory = Path(temp)
        source = directory / "test.bin"
        # Byte deterministici e file non vuoto.
        source.write_bytes(bytes(range(256)) * 40)
        expected = sha256_file(source)

        hdfs = HDFSClient()
        remote = "/benchmark/smoke/test.bin"
        try:
            hdfs.upload(source, remote)
            downloaded = directory / "from_hdfs.bin"
            hdfs.download(remote, downloaded)
            if sha256_file(downloaded) != expected:
                raise AssertionError("SHA-256 HDFS non corrispondente")
            results["HDFS"] = expected
        finally:
            # Non mascherare eventuali errori di upload/download con il cleanup.
            hdfs.close()

        ozone = OzoneClient()
        ozone.ensure_bucket()
        key = "smoke/test.bin"
        ozone.upload(source, key)
        downloaded = directory / "from_ozone.bin"
        ozone.download(key, downloaded)
        if sha256_file(downloaded) != expected:
            raise AssertionError("SHA-256 Ozone non corrispondente")
        results["Ozone"] = expected
    return results


if __name__ == "__main__":
    for system, checksum in run_smoke_test().items():
        print(f"{system}: upload/download/integrità OK | SHA-256: {checksum}")
