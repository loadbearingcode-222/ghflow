"""S3 objects on SeaweedFS, through pyarrow's filesystem (ADR-0001: signed, path style).

Paths are `bucket/key`. An object becomes visible only when its upload completes, so a reader
never sees half a file; writing the same path again replaces it (never a second object).
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from urllib.parse import urlsplit

from pyarrow import fs


@dataclass(frozen=True)
class S3Config:
    endpoint: str  # e.g. http://seaweedfs:8333 inside compose, http://127.0.0.1:39000 on the host
    access_key: str
    secret_key: str

    @classmethod
    def from_env(cls) -> S3Config:
        """S3_ENDPOINT, S3_ACCESS_KEY and S3_SECRET_KEY, as compose sets them for Airflow."""
        return cls(
            os.environ["S3_ENDPOINT"], os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"]
        )


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class Store:
    def __init__(self, config: S3Config) -> None:
        url = urlsplit(config.endpoint)
        self.fs = fs.S3FileSystem(
            access_key=config.access_key,
            secret_key=config.secret_key,
            endpoint_override=url.netloc,
            scheme=url.scheme,
            region="us-east-1",
            force_virtual_addressing=False,
        )

    def put(self, path: str, data: bytes) -> str:
        """Write (or replace) one object; returns the sha256 of what was written."""
        with self.fs.open_output_stream(path) as out:
            out.write(data)
        return sha256(data)

    def get(self, path: str) -> bytes:
        with self.fs.open_input_stream(path) as source:
            return bytes(source.read())

    def exists(self, path: str) -> bool:
        return bool(self.fs.get_file_info(path).type == fs.FileType.File)

    def files(self, prefix: str) -> list[str]:
        """Every object under a prefix, sorted; empty if the prefix does not exist."""
        selector = fs.FileSelector(prefix, recursive=True, allow_not_found=True)
        infos = self.fs.get_file_info(selector)
        return sorted(i.path for i in infos if i.type == fs.FileType.File)
