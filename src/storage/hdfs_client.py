"""Client WebHDFS per trasferimenti di file in streaming.

Il client opera dall'interno della rete Docker del laboratorio; i redirect
WebHDFS verso i DataNode devono essere risolvibili dal container chiamante.
"""
from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote, urlsplit

import requests


class HDFSClient:
    """Gestisce upload, download, controllo esistenza e rimozione via WebHDFS."""

    def __init__(
        self,
        base_url: str | None = None,
        user: str = "hadoop",
        timeout: tuple[float, float] = (10.0, 600.0),
        chunk_size: int = 1024 * 1024,
    ) -> None:
        self.base_url = (base_url or os.getenv("HDFS_WEBHDFS_URL", "http://namenode:9870")).rstrip("/")
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError("L'endpoint WebHDFS deve iniziare con http:// o https://")
        self.user = user
        self.timeout = timeout
        if chunk_size < 1:
            raise ValueError("chunk_size deve essere positivo")
        self.chunk_size = chunk_size
        self.session = requests.Session()
        # I nomi DNS interni Docker non devono attraversare proxy di sistema.
        self.session.trust_env = False

    @staticmethod
    def _path(remote_path: str) -> str:
        if not remote_path or not remote_path.startswith("/"):
            raise ValueError("Il percorso HDFS deve essere assoluto, ad esempio /benchmark/file.bin")
        return quote(remote_path, safe="/")

    def _url(self, remote_path: str) -> str:
        return f"{self.base_url}/webhdfs/v1{self._path(remote_path)}"

    def _params(self, op: str, **extra: str) -> dict[str, str]:
        return {"op": op, "user.name": self.user, **extra}

    @staticmethod
    def _check(response: requests.Response, accepted: tuple[int, ...]) -> None:
        if response.status_code not in accepted:
            detail = response.text[:800]
            raise RuntimeError(f"WebHDFS HTTP {response.status_code}: {detail}")

    def mkdir(self, remote_dir: str) -> None:
        """Crea ricorsivamente la directory HDFS, se assente."""
        with self.session.put(
            self._url(remote_dir), params=self._params("MKDIRS"), timeout=self.timeout
        ) as response:
            self._check(response, (200,))
            if response.json().get("boolean") is not True:
                raise RuntimeError(f"Creazione directory HDFS fallita: {remote_dir}")

    def exists(self, remote_path: str) -> bool:
        """Controlla se esiste un file/directory HDFS."""
        with self.session.get(
            self._url(remote_path), params=self._params("GETFILESTATUS"), timeout=self.timeout
        ) as response:
            if response.status_code == 404:
                return False
            self._check(response, (200,))
            return True

    def upload(self, local_path: str | Path, remote_path: str, overwrite: bool = True) -> None:
        """Carica un file senza caricarlo per intero in RAM.

        WebHDFS richiede due richieste: PUT al NameNode (307) e PUT con body
        verso il DataNode. I byte vengono letti in streaming dal file locale.
        """
        local = Path(local_path)
        if not local.is_file():
            raise FileNotFoundError(local)
        remote_parent = remote_path.rsplit("/", 1)[0] or "/"
        self.mkdir(remote_parent)
        with self.session.put(
            self._url(remote_path),
            params=self._params("CREATE", overwrite=str(overwrite).lower()),
            allow_redirects=False,
            timeout=self.timeout,
        ) as response:
            self._check(response, (307,))
            destination = response.headers.get("Location")
        if not destination:
            raise RuntimeError("WebHDFS CREATE non ha restituito Location del DataNode")
        with local.open("rb") as source:
            with self.session.put(destination, data=source, timeout=self.timeout) as response:
                self._check(response, (201,))

    def download(self, remote_path: str, local_path: str | Path) -> None:
        """Scarica un file in streaming e sostituisce il file locale solo a download riuscito."""
        destination = Path(local_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(destination.name + ".part")
        try:
            with self.session.get(
                self._url(remote_path),
                params=self._params("OPEN"),
                allow_redirects=True,
                stream=True,
                timeout=self.timeout,
            ) as response:
                self._check(response, (200,))
                with partial.open("wb") as target:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if chunk:
                            target.write(chunk)
            partial.replace(destination)
        except Exception:
            partial.unlink(missing_ok=True)
            raise

    def delete(self, remote_path: str, recursive: bool = False) -> bool:
        """Elimina un percorso HDFS, restituendo True se cancellato."""
        with self.session.delete(
            self._url(remote_path),
            params=self._params("DELETE", recursive=str(recursive).lower()),
            timeout=self.timeout,
        ) as response:
            self._check(response, (200,))
            return bool(response.json().get("boolean"))

    def close(self) -> None:
        """Rilascia le connessioni HTTP."""
        self.session.close()
