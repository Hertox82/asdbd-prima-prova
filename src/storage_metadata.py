"""Controlli non cronometrati dei metadati HDFS e Ozone.

Le informazioni sui blocchi HDFS sono richieste via WebHDFS; se
l'endpoint della versione installata non le espone, il report lo dichiara.
"""
from __future__ import annotations

from pathlib import Path
import json
from .storage import HDFSClient, OzoneClient


def inspect_hdfs(remote_path: str, *, client: HDFSClient | None = None) -> dict:
    """Verifica presenza, dimensioni, block size e (se supportate) posizioni blocchi."""
    owned = client is None
    client = client or HDFSClient()
    try:
        if not client.exists(remote_path):
            raise FileNotFoundError(f"File HDFS assente: {remote_path}")
        with client.session.get(client._url(remote_path), params=client._params("GETFILESTATUS"), timeout=client.timeout) as response:
            client._check(response, (200,))
            status = response.json()["FileStatus"]
        length = int(status["length"])
        block_size = int(status["blockSize"])
        result = {
            "sistema": "HDFS", "percorso": remote_path,
            "presente": True, "dimensione_byte": length,
            "tipo": status.get("type"), "owner": status.get("owner"),
            "replica": status.get("replication"),
            "block_size_byte": block_size,
            "blocchi_attesi": (length + block_size - 1) // block_size if block_size else None,
            "modifica_epoch_ms": status.get("modificationTime"),
        }
        # Alcune versioni WebHDFS espongono GETFILEBLOCKLOCATIONS, altre no.
        try:
            with client.session.get(client._url(remote_path), params=client._params("GETFILEBLOCKLOCATIONS", offset="0", length=str(length)), timeout=client.timeout) as response:
                if response.ok:
                    payload=response.json()
                    result["block_locations"] = payload.get("BlockLocations", payload)
                else:
                    result["block_locations_note"] = f"API posizioni blocchi non disponibile: HTTP {response.status_code}"
        except Exception as exc:
            result["block_locations_note"] = f"Localizzazione non disponibile: {type(exc).__name__}: {exc}"
        return result
    finally:
        if owned:
            client.close()


def inspect_ozone(key: str, *, bucket: str = "asdbd-benchmark", client: OzoneClient | None = None) -> dict:
    """Verifica bucket, key e metadati di un oggetto Ozone via S3 Gateway."""
    client = client or OzoneClient(bucket=bucket)
    # HEAD bucket fallisce esplicitamente se non esiste: non creiamo risorse qui.
    client.s3.head_bucket(Bucket=bucket)
    response = client.s3.head_object(Bucket=bucket, Key=key)
    return {
        "sistema": "Ozone", "bucket": bucket, "key": key,
        "bucket_presente": True, "oggetto_presente": True,
        "dimensione_byte": int(response["ContentLength"]),
        "ultima_modifica": response.get("LastModified").isoformat() if response.get("LastModified") else None,
        "etag": response.get("ETag"),
        "content_type": response.get("ContentType"),
        "nota_etag": "L'ETag S3 non e' necessariamente MD5, soprattutto per multipart; integrita' verificata con SHA-256 nel benchmark.",
        "nota_container": "S3 HEAD non espone direttamente gli ID dei container fisici Ozone; pipeline RATIS/ONE verificata amministrativamente nel laboratorio.",
    }


def inspect_benchmark_run(raw_csv: str | Path, *, bucket: str = "asdbd-benchmark", output_json: str | Path | None = None) -> list[dict]:
    """Controlla un file per dimensione/sistema, usando solo i dati del run corrente."""
    import pandas as pd
    df = pd.read_csv(raw_csv)
    df = df[(df["verified"].astype(str).str.lower() == "true") & (df["repeat"] == 1)]
    if df.empty:
        raise ValueError("Nessun upload verificato con ripetizione 1")
    found=[]
    hdfs=HDFSClient()
    ozone=OzoneClient(bucket=bucket)
    try:
        for _, row in df.iterrows():
            path=str(row["remote_path"])
            entry = inspect_hdfs(path, client=hdfs) if row["system"] == "HDFS" else inspect_ozone(path, bucket=bucket, client=ozone)
            if int(entry["dimensione_byte"]) != int(row["size_bytes"]):
                raise RuntimeError(f"Dimensione remota errata: {row['system']} {path}")
            entry["dimensione"] = str(row["size_label"])
            entry["dimensione_verificata"] = True
            found.append(entry)
    finally:
        hdfs.close()
    if output_json:
        out=Path(output_json); out.parent.mkdir(parents=True,exist_ok=True)
        out.write_text(json.dumps(found,indent=2,ensure_ascii=False,default=str),encoding="utf-8")
    return found
