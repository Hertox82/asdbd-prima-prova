"""Client di storage utilizzati dal laboratorio ASDBD."""

from .hdfs_client import HDFSClient
from .ozone_client import OzoneClient

__all__ = ["HDFSClient", "OzoneClient"]
