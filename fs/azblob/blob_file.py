import io
import os
import tempfile

from fs.mode import Mode


class BlobFile(io.IOBase):
    """Proxy for a blob file."""

    @classmethod
    def factory(cls, blob, mode):
        """Create a BlobFile backed with a temporary file."""
        _temp_file = tempfile.TemporaryFile()
        proxy = cls(_temp_file, blob, mode)
        return proxy

    def __repr__(self):
        return f"BlobFile({self.blob.blob_name}, {self.mode})"

    def __init__(self, f, blob, mode):
        self._f = f
        self.blob = blob
        self.mode = Mode(mode)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def raw(self):
        return self._f

    def close(self):
        if self.mode.writing:
            self.seek(os.SEEK_SET)
            self.blob.upload_blob(self.raw, overwrite=True)
        self._f.close()

    @property
    def closed(self):
        return self._f.closed

    def fileno(self):
        return self._f.fileno()

    def flush(self):
        return self._f.flush()

    def readable(self):
        return self.mode.reading

    def readline(self, limit=-1):
        return self._f.readline(limit)

    def seek(self, offset, whence=os.SEEK_SET):
        if whence not in (os.SEEK_CUR, os.SEEK_END, os.SEEK_SET):
            raise ValueError("invalid value for 'whence'")
        self._f.seek(offset, whence)
        return self._f.tell()

    def seekable(self):
        return True

    def tell(self):
        return self._f.tell()

    def writable(self):
        return self.mode.writing

    def writelines(self, lines):
        return self._f.writelines(lines)

    def read(self, n=-1):
        if not self.mode.reading:
            raise OSError("not open for reading")
        return self._f.read(n)

    def readall(self):
        return self._f.read()

    def readinto(self, b):
        return self._f.readinto(b)

    def write(self, b):
        if not self.mode.writing:
            raise OSError("not open for writing")
        self._f.write(b)
        return len(b)

    def truncate(self, size=None):
        if size is None:
            size = self._f.tell()
        self._f.truncate(size)
        return size

    def __next__(self):
        line = self._f.readline()
        if not line:
            raise StopIteration
        return line
