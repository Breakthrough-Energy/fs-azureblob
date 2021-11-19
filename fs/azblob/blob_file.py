import io
import typing
from typing import Optional

from azure.storage.blob import BlobClient

from fs.azblob.error_tools import blobfs_errors

Bytes = Optional[bytes]
EMPTY_BYTES = b""


class BlobFile(io.RawIOBase):
    def __init__(self, client: BlobClient, mode=None):
        self.client = client
        self.mode = mode
        self._reader: BlobStreamReader = None  # type: ignore
        self._writer: BlobWriter = None  # type: ignore
        self._closed = False

    def flush(self) -> None:
        if not self._closed and self.writable():
            with blobfs_errors(self.client.blob_name):
                self.writer.commit()
                self._closed = True

    def readable(self) -> bool:
        return self.mode.reading

    def writable(self) -> bool:
        return self.mode.writing

    @property
    def reader(self) -> "BlobStreamReader":
        if self._reader is None:
            with blobfs_errors(self.client.blob_name):
                self._reader = BlobStreamReader(self.client.download_blob())
        return self._reader

    @property
    def writer(self) -> "BlobWriter":
        if not self.writable():
            raise ValueError("BlobFile must be opened in writing mode")
        if self._writer is None:
            self._writer = BlobWriter(self.mode, self.client, self.readall)
        return self._writer

    def write(self, data) -> int:
        self.writer.write(data)
        return len(data)

    @typing.no_type_check
    def readinto(self, b: bytearray) -> Optional[int]:
        result = self.reader.get_bytes(len(b))
        if result is None:
            return None
        n_bytes = len(result)
        b[:n_bytes] = result
        return n_bytes


class BlobWriter:
    def __init__(self, mode, client, download) -> None:
        self.client = client
        self.mode = mode
        self._download = download
        self.buf = bytearray()

    def write(self, data):
        self.buf.extend(data)

    def commit(self):
        if not self.client.exists():
            content = EMPTY_BYTES
        elif self.mode.appending:
            content = self._download()
        else:
            content = EMPTY_BYTES

        content = content + bytes(self.buf)
        self.client.upload_blob(content, overwrite=True)


class BlobStreamReader:
    def __init__(self, stream) -> None:
        self.chunks = stream.chunks()
        self.leftover = bytearray()
        self.eof = False

    def readline(self, size: int = -1) -> bytes:
        newline = b"\n"
        if self.eof:
            return EMPTY_BYTES
        while newline not in self.leftover:
            try:
                self.leftover.extend(next(self.chunks))
            except StopIteration:
                self.eof = True
                return bytes(self.leftover)
        idx_linebreak = self.leftover.find(newline)
        n = None
        if size != -1 and idx_linebreak > size:
            n = size
        else:
            n = idx_linebreak + 1
        curr, self.leftover = self.leftover[:n], self.leftover[n:]
        return bytes(curr)

    def get_bytes(self, size: int) -> Bytes:
        n = size
        if not self.eof:
            while len(self.leftover) < n:
                try:
                    self.leftover.extend(next(self.chunks))
                except StopIteration:
                    self.eof = True
                    break
        curr, self.leftover = self.leftover[:n], self.leftover[n:]
        return bytes(curr)
