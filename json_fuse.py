from __future__ import annotations

import errno
import json
import os
import stat
import time
from typing import Any

import mfusepy as fuse


class JSONFuse(fuse.Operations):
    """
    Read-only FUSE projection of a static JSON document.

    Rules:
    - dict => directory of keys
    - list => directory of numeric indexes
    - scalar => readable file
    """

    use_ns = True

    def __init__(self, data: Any) -> None:
        self.data = data
        self._dir_mode = stat.S_IFDIR | 0o755
        self._file_mode = stat.S_IFREG | 0o444

    def access(self, path: str, mode: int) -> int:
        if mode & os.W_OK:
            raise fuse.FuseOSError(errno.EROFS)
        self.getattr(path)
        return 0

    def getattr(self, path: str, fh: int | None = None) -> dict[str, Any]:
        now = time.time()
        node = self._resolve_node(self._normalize_path(path))
        if node is None:
            raise fuse.FuseOSError(errno.ENOENT)
        if isinstance(node, (dict, list)):
            return {
                "st_mode": self._dir_mode,
                "st_nlink": 2,
                "st_size": 0,
                "st_ctime": now,
                "st_mtime": now,
                "st_atime": now,
            }
        content = self._encode_scalar(node)
        return {
            "st_mode": self._file_mode,
            "st_nlink": 1,
            "st_size": len(content),
            "st_ctime": now,
            "st_mtime": now,
            "st_atime": now,
        }

    def readdir(self, path: str, fh: int) -> list[str]:
        node = self._resolve_node(self._normalize_path(path))
        if node is None:
            raise fuse.FuseOSError(errno.ENOENT)
        entries = [".", ".."]
        if isinstance(node, dict):
            entries.extend(sorted(node.keys()))
            return entries
        if isinstance(node, list):
            entries.extend(str(index) for index in range(len(node)))
            return entries
        raise fuse.FuseOSError(errno.ENOTDIR)

    def open(self, path: str, flags: int) -> int:
        if flags & os.O_WRONLY or flags & os.O_RDWR:
            raise fuse.FuseOSError(errno.EROFS)
        node = self._resolve_node(self._normalize_path(path))
        if node is None:
            raise fuse.FuseOSError(errno.ENOENT)
        if isinstance(node, (dict, list)):
            raise fuse.FuseOSError(errno.EISDIR)
        return 0

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        node = self._resolve_node(self._normalize_path(path))
        if node is None:
            raise fuse.FuseOSError(errno.ENOENT)
        if isinstance(node, (dict, list)):
            raise fuse.FuseOSError(errno.EISDIR)
        content = self._encode_scalar(node)
        return content[offset : offset + size]

    def statfs(self, path: str) -> dict[str, int]:
        return {
            "f_bsize": 4096,
            "f_frsize": 4096,
            "f_blocks": 1,
            "f_bfree": 0,
            "f_bavail": 0,
            "f_files": 4096,
            "f_ffree": 0,
            "f_favail": 0,
            "f_flag": 0,
            "f_namemax": 255,
        }

    def _normalize_path(self, path: str) -> str:
        normalized = os.path.normpath(path)
        if not normalized.startswith("/"):
            normalized = f"/{normalized}"
        return normalized

    def _resolve_node(self, path: str) -> Any | None:
        if path == "/":
            return self.data
        parts = [part for part in path.strip("/").split("/") if part]
        node: Any = self.data
        for part in parts:
            if isinstance(node, dict):
                if part not in node:
                    return None
                node = node[part]
                continue
            if isinstance(node, list):
                if not part.isdigit():
                    return None
                index = int(part)
                if index < 0 or index >= len(node):
                    return None
                node = node[index]
                continue
            return None
        return node

    def _encode_scalar(self, value: Any) -> bytes:
        if isinstance(value, bool):
            return ("true\n" if value else "false\n").encode("utf-8")
        if value is None:
            return b"null\n"
        if isinstance(value, (int, float)):
            return f"{value}\n".encode("utf-8")
        if isinstance(value, str):
            return value.encode("utf-8") + b"\n"
        return json.dumps(value, indent=2, sort_keys=True).encode("utf-8") + b"\n"

