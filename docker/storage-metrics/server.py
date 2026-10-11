"""API HTTP locale alla rete Compose, con accesso in sola lettura ai volumi Docker.

Riporta sia la lunghezza apparente sia i blocchi allocati dal filesystem
(st_blocks * 512). Non restituisce nomi di file o contenuti.
"""
from __future__ import annotations

import json
import os
import stat
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOTS = {
    "hdfs_name": "/mnt/hdfs/name",
    "hdfs_dn1": "/mnt/hdfs/dn1",
    "hdfs_dn2": "/mnt/hdfs/dn2",
    "hdfs_dn3": "/mnt/hdfs/dn3",
    "ozone_scm": "/mnt/ozone/scm",
    "ozone_om": "/mnt/ozone/om",
    "ozone_dn": "/mnt/ozone/dn",
    "ozone_recon": "/mnt/ozone/recon",
}


def measure(root: str) -> dict[str, int]:
    """Somma dimensioni logiche e byte allocati, senza seguire link simbolici."""
    apparent = allocated = nfiles = 0
    todo = [Path(root)]
    while todo:
        path = todo.pop()
        # os.scandir non segue symlink nelle directory.
        with os.scandir(path) as entries:
            for e in entries:
                info = e.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    todo.append(Path(e.path))
                elif stat.S_ISREG(info.st_mode):
                    apparent += info.st_size
                    allocated += getattr(info, "st_blocks", 0) * 512
                    nfiles += 1
    return {"apparent_bytes": apparent, "allocated_bytes": allocated, "file_count": nfiles}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/snapshot":
            self.send_error(404)
            return
        try:
            payload = {key: measure(root) for key, root in ROOTS.items()}
            content = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except (OSError, PermissionError) as exc:
            self.send_error(503, str(exc))

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8765), Handler).serve_forever()
