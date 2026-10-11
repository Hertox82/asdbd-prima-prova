"""Analisi incrementale dell'occupazione dei volumi HDFS e Ozone.

Da eseguire nel Jupyter container, attraverso un sidecar in sola lettura.
L'analisi misura upload dedicati, senza ripetere il benchmark prestazionale.
"""
from __future__ import annotations

import csv
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import requests

from .file_generator import FILE_SIZES, generate_benchmark_files
from .storage import HDFSClient, OzoneClient

ROOT = Path(__file__).resolve().parent.parent
HDFS_DATA = ("hdfs_dn1", "hdfs_dn2", "hdfs_dn3")
HDFS_META = ("hdfs_name",)
OZONE_DATA = ("ozone_dn",)
OZONE_META = ("ozone_om", "ozone_scm")
OZONE_OBSERVABILITY = ("ozone_recon",)
COLUMNS = ["run_id", "size_label", "size_bytes", "system", "logical_bytes", "data_delta_allocated_bytes", "metadata_delta_allocated_bytes", "total_delta_allocated_bytes", "overhead_estimate_pct", "negative_delta", "storage_id"]


def snapshot(endpoint: str | None = None) -> dict:
    """Legge le dimensioni dei volumi montati in sola lettura nel sidecar."""
    url = (endpoint or os.environ.get("STORAGE_METRICS_URL", "http://storage-metrics:8765")).rstrip("/")
    response = requests.get(f"{url}/snapshot", timeout=180)
    response.raise_for_status()
    payload = response.json()
    required = set(HDFS_DATA + HDFS_META + OZONE_DATA + OZONE_META)
    if not required.issubset(payload):
        raise RuntimeError(f"Volumi non rilevati: {sorted(required - set(payload))}")
    return payload


def _sum_allocated(value: dict, names: tuple[str, ...]) -> int:
    return sum(int(value[name]["allocated_bytes"]) for name in names)


def compare(before: dict, after: dict, system: str, size_bytes: int) -> dict:
    """Calcola il delta dello spazio allocato; negativo = riutilizzo/pulizia."""
    data, metadata = ((HDFS_DATA, HDFS_META) if system == "HDFS" else (OZONE_DATA, OZONE_META))
    delta_data = _sum_allocated(after, data) - _sum_allocated(before, data)
    delta_meta = _sum_allocated(after, metadata) - _sum_allocated(before, metadata)
    total = delta_data + delta_meta
    return {
        "logical_bytes": size_bytes,
        "data_delta_allocated_bytes": delta_data,
        "metadata_delta_allocated_bytes": delta_meta,
        "total_delta_allocated_bytes": total,
        "overhead_estimate_pct": round(100 * (total - size_bytes) / size_bytes, 3),
        "negative_delta": total < 0,
    }


def run_storage_analysis(*, results_dir: str | Path = ROOT / "results", run_id: str | None = None,
                         settle_seconds: float = 2.0, keep_remote: bool = True) -> dict:
    """Esegue un upload dedicato per 4 dimensioni × 2 sistemi e misura il delta.

    Usa chiavi remote uniche. Non misura le prestazioni; il checksum dei dati
    è stato controllato dai client nel benchmark. Per l'analisi dello spazio,
    l'upload è verificato tramite exists e la dimensione locale è nota.

    Per evitare di compromettere i volumi, keep_remote=True è il default.
    Se False vengono rimossi gli oggetti *dopo* la fotografia finale: la
    pulizia potrebbe essere asincrona e non fa parte della misura.
    """
    if settle_seconds < 0:
        raise ValueError("settle_seconds deve essere >= 0")
    folder = Path(results_dir)
    folder.mkdir(parents=True, exist_ok=True)
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6]
    if not all(c.isalnum() or c in "_-" for c in run_id):
        raise ValueError("run_id non valido")
    destination = folder / f"storage_analysis_{run_id}.csv"
    if destination.exists():
        raise FileExistsError("Un'analisi esiste già con questo run_id. Creare una nuova esecuzione.")
    entries = {str(e["name"]): e for e in generate_benchmark_files()}
    hdfs = HDFSClient()
    ozone = OzoneClient(bucket="asdbd-benchmark")
    records = []
    try:
        snapshot()  # verifica preliminare del sidecar
        ozone.ensure_bucket()
        hdfs.mkdir(f"/benchmark/storage-analysis/{run_id}")
        with destination.open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=COLUMNS)
            writer.writeheader()
            for label, size_bytes in FILE_SIZES.items():
                source = Path(str(entries[label]["path"]))
                if source.stat().st_size != size_bytes:
                    raise ValueError(f"Dimensione imprevista: {source}")
                for system in ("HDFS", "Ozone"):
                    client = hdfs if system == "HDFS" else ozone
                    remote = (f"/benchmark/storage-analysis/{run_id}/{label}.bin" if system == "HDFS"
                              else f"storage-analysis/{run_id}/{label}.bin")
                    before = snapshot()
                    print(f"[STORAGE] {system} {label}: upload e rilevazione...", flush=True)
                    try:
                        client.upload(source, remote)
                        if not client.exists(remote):
                            raise RuntimeError(f"File remoto non presente: {remote}")
                        time.sleep(settle_seconds)
                        after = snapshot()
                        row = {"run_id": run_id, "size_label": label, "size_bytes": size_bytes,
                               "system": system, "storage_id": remote,
                               **compare(before, after, system, size_bytes)}
                        writer.writerow(row)
                        output.flush()
                        records.append(row)
                    finally:
                        if not keep_remote:
                            try:
                                client.delete(remote)
                            except Exception as exc:
                                print(f"[AVVISO] Pulizia fallita: {exc}", flush=True)
        return {"run_id": run_id, "csv": str(destination), "rows": records}
    finally:
        hdfs.close()


def plot_storage_analysis(csv_path: str | Path) -> Path:
    """Grafico del delta dati+metadati, con valori che possono essere negativi."""
    import matplotlib.pyplot as plt
    with Path(csv_path).open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    x = list(range(len(FILE_SIZES)))
    width = .38
    fig, ax = plt.subplots(figsize=(9, 5))
    for system, shift in (("HDFS", -width / 2), ("Ozone", width / 2)):
        lookup = {r["size_label"]: float(r["total_delta_allocated_bytes"]) / 1024**2
                  for r in rows if r["system"] == system}
        ax.bar([i + shift for i in x], [lookup.get(k, float("nan")) for k in FILE_SIZES], width, label=system)
    ax.set_xticks(x, list(FILE_SIZES))
    ax.set_ylabel("Delta stimato dati + metadati (MiB allocati)")
    ax.set_title("Variazione di spazio allocato per upload dedicato")
    ax.grid(axis="y", alpha=.2)
    ax.legend()
    fig.tight_layout()
    output = Path(csv_path).with_suffix(".png")
    fig.savefig(output, dpi=150)
    plt.close(fig)
    return output


if __name__ == "__main__":
    output = run_storage_analysis()
    print(json.dumps({"csv": output["csv"], "plot": str(plot_storage_analysis(output["csv"]))}, indent=2))
