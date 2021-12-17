import io
import os
import tempfile
from typing import Optional

from fs.mode import Mode


class BlobFile(io.IOBase):
    """Proxy for a blob file. Implements the standard file api.
    See https://docs.python.org/3/library/io.html#io.IOBase
    """

    @classmethod
    def factory(cls, blob, mode):
        """Create a BlobFile backed with a temporary file."""
        _temp_file = tempfile.TemporaryFile()
        proxy = cls(_temp_file, blob, mode)
        return proxy

    def __repr__(self):
        return f"BlobFile({self._blob.blob_name}, {self.mode})"

    def __init__(self, f, blob, mode):
        self._f = f
        self._blob = blob
        self.mode = Mode(mode)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def raw(self):
        return self._f

    def close(self) -> None:
        if self.mode.writing:
            self.seek(0)
            self._blob.upload_blob(self.raw, overwrite=True)
        self._f.close()

    @property
    def closed(self) -> bool:
        return self._f.closed

    def fileno(self) -> int:
        return self._f.fileno()

    def flush(self) -> None:
        return self._f.flush()

    def readable(self) -> bool:
        return self.mode.reading

    def readline(self, limit: Optional[int] = -1) -> bytes:
        return self._f.readline(limit)

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence not in (os.SEEK_CUR, os.SEEK_END, os.SEEK_SET):
            raise ValueError("invalid value for 'whence'")
        self._f.seek(offset, whence)
        return self._f.tell()

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._f.tell()

    def writable(self) -> bool:
        return self.mode.writing

    def writelines(self, lines) -> None:
        return self._f.writelines(lines)

    def read(self, n: int = -1) -> bytes:
        if not self.mode.reading:
            raise OSError("not open for reading")
        return self._f.read(n)

    def readall(self) -> bytes:
        return self._f.read()

    def readinto(self, b) -> int:
        return self._f.readinto(b)

    def write(self, b) -> int:
        if not self.mode.writing:
            raise OSError("not open for writing")
        self._f.write(b)
        return len(b)

    def truncate(self, size: Optional[int] = None) -> int:
        if size is None:
            size = self._f.tell()
        self._f.truncate(size)
        return size

    def __next__(self) -> bytes:
        line = self._f.readline()
        if not line:
            raise StopIteration
        return line
