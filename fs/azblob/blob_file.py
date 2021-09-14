import io
import typing
from typing import Iterator, List

from azure.storage.blob import BlobClient, BlobType


class BlobFile(io.RawIOBase):
    def __init__(self, client: BlobClient, mode=None):
        self.client = client
        self.mode = mode
        self._reader: ChunkReader = None  # type: ignore

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return False

    def readable(self) -> bool:
        return self.mode.reading

    def writable(self) -> bool:
        return self.mode.writing

    def seekable(self) -> bool:
        return False

    def write(self, data) -> int:
        self.client.upload_blob(data, blob_type=BlobType.AppendBlob)
        return len(data)

    def read(self, n: int = -1) -> bytes:
        if n == -1:
            return self.readall()

        if self._reader is None:
            self._reader = ChunkReader(self.client.download_blob())
        return self._reader.get_next(n)

    def readall(self) -> bytes:
        stream = self.client.download_blob()
        return stream.content_as_bytes()

    @typing.no_type_check
    def readinto(self, b: bytearray) -> int:
        before = len(b)
        b.extend(self.readall())
        return len(b) - before

    def readline(self, limit: int = None) -> bytes:
        raise NotImplementedError

    def readlines(self, hint: int = None) -> List[bytes]:
        raise NotImplementedError

    def writelines(self, lines) -> None:
        raise NotImplementedError

    def __iter__(self) -> Iterator[bytes]:
        # TODO
        raise NotImplementedError


class ChunkReader:
    def __init__(self, stream) -> None:
        self.chunks = stream.chunks()
        self.leftover = bytearray()
        self.eof = False

    def get_next(self, size):
        if self.eof:
            return None
        n = size
        while len(self.leftover) < n:
            try:
                self.leftover.extend(next(self.chunks))
            except StopIteration:
                self.eof = True
                return bytes(self.leftover)
        curr, self.leftover = self.leftover[:n], self.leftover[n:]
        assert len(curr) == n
        return bytes(curr)
