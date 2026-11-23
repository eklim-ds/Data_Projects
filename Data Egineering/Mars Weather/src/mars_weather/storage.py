"""A tiny object-store abstraction.

The pipeline only needs ``put`` / ``get`` / ``list`` on opaque keys, which is exactly the
surface area of GCS / S3. ``LocalObjectStore`` implements it on a local directory so the
whole project runs on a laptop; a ``GCSObjectStore`` would implement the same three methods
without any change to the ingestion code.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path, PurePosixPath
from typing import List, Protocol


class ObjectStore(Protocol):
    def put(self, key: str, data: bytes) -> str: ...

    def get(self, key: str) -> bytes: ...

    def list(self, prefix: str = "") -> List[str]: ...


class InvalidKeyError(ValueError):
    """Raised when an object key could escape the store root or is otherwise malformed."""


class LocalObjectStore:
    """Object store backed by a directory. Keys are POSIX-style, like bucket object names."""

    def __init__(self, root: Path):
        self.root = Path(root).resolve()

    def _path_for(self, key: str) -> Path:
        pure = PurePosixPath(key)
        if not key or pure.is_absolute() or ".." in pure.parts:
            raise InvalidKeyError(f"Invalid object key: {key!r}")
        path = (self.root / pure).resolve()
        if self.root != path and self.root not in path.parents:
            raise InvalidKeyError(f"Object key escapes the store root: {key!r}")
        return path

    def uri(self, key: str) -> str:
        # Human-readable (unlike Path.as_uri(), which would percent-encode the '=' in 'dt=...').
        return f"file://{self._path_for(key).as_posix()}"

    def put(self, key: str, data: bytes) -> str:
        """Write ``data`` atomically (temp file + rename) so readers never see a partial object."""
        path = self._path_for(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".part")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            os.replace(tmp_name, path)
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise
        return self.uri(key)

    def get(self, key: str) -> bytes:
        return self._path_for(key).read_bytes()

    def list(self, prefix: str = "") -> List[str]:
        base = self.root
        keys = [
            path.relative_to(base).as_posix()
            for path in base.rglob("*")
            if path.is_file() and not path.name.startswith(".tmp-")
        ]
        return sorted(key for key in keys if key.startswith(prefix))
