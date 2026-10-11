"""Client Apache Ozone tramite S3 Gateway, con trasferimenti multipart/streaming."""
from __future__ import annotations

import os
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError


class OzoneClient:
    """Gestisce bucket e oggetti Ozone via API compatibili S3."""

    def __init__(
        self,
        endpoint_url: str | None = None,
        bucket: str = "asdbd-benchmark",
        chunk_size: int = 8 * 1024 * 1024,
    ) -> None:
        self.bucket = bucket
        self.endpoint_url = endpoint_url or os.getenv("OZONE_S3_ENDPOINT", "http://s3g:9878")
        if chunk_size < 5 * 1024 * 1024:
            raise ValueError("Il chunk multipart S3 deve essere >= 5 MiB")
        self.transfer_config = TransferConfig(
            multipart_threshold=chunk_size,
            multipart_chunksize=chunk_size,
            use_threads=False,  # Concorrenza disabilitata: benchmark più controllabile.
        )
        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            config=Config(
                s3={"addressing_style": "path"},
                connect_timeout=10,
                read_timeout=600,
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )

    def ensure_bucket(self) -> None:
        """Crea il bucket se non esiste; non nasconde altri errori S3."""
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in ("404", "NoSuchBucket", "NotFound") or status == 404:
                self.s3.create_bucket(Bucket=self.bucket)
            else:
                raise

    def exists(self, key: str) -> bool:
        """Controlla l'esistenza di un oggetto nel bucket."""
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in ("404", "NoSuchKey", "NotFound") or status == 404:
                return False
            raise
        return True

    def upload(self, local_path: str | Path, key: str) -> None:
        """Carica un file usando multipart per file grandi, senza leggerlo tutto in RAM."""
        local = Path(local_path)
        if not local.is_file():
            raise FileNotFoundError(local)
        self.s3.upload_file(str(local), self.bucket, key, Config=self.transfer_config)

    def download(self, key: str, local_path: str | Path) -> None:
        """Scarica l'oggetto; non lascia un file finale parziale in caso di errore."""
        destination = Path(local_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(destination.name + ".part")
        try:
            self.s3.download_file(self.bucket, key, str(partial), Config=self.transfer_config)
            partial.replace(destination)
        except Exception:
            partial.unlink(missing_ok=True)
            raise

    def delete(self, key: str) -> None:
        """Elimina un oggetto dal bucket; S3 considera riuscita anche l'assenza dell'oggetto."""
        self.s3.delete_object(Bucket=self.bucket, Key=key)
