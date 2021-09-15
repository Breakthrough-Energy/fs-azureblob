import io
import typing
from typing import Iterator, List, Optional

from azure.storage.blob import BlobClient, BlobType

Bytes = Optional[bytes]
EMPTY_BYTES = b""


class BlobFile(io.RawIOBase):
    def __init__(self, client: BlobClient, mode=None):
        self.client = client
        self.mode = mode
        self._reader: BlobStreamReader = None  # type: ignore

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

    def read(self, n: int = -1) -> Bytes:
        if n == -1:
            return self.readall()

        if self._reader is None:
            self._reader = BlobStreamReader(self.client.download_blob())
        return self._reader.get_bytes(n)

    def readall(self) -> bytes:
        stream = self.client.download_blob()
        return stream.content_as_bytes()

    @typing.no_type_check
    def readinto(self, b: bytearray) -> int:
        before = len(b)
        b.extend(self.readall())
        return len(b) - before

    def readline(self, size: int = None) -> bytes:
        if self._reader is None:
            self._reader = BlobStreamReader(self.client.download_blob())
        result = self._reader.readline()
        if size == -1 or result == EMPTY_BYTES:
            return result
        return result[:size]

    def readlines(self, hint: int = None) -> List[bytes]:
        def _is_complete(lines, hint):
            if hint is None or hint <= 0:
                return False
            return len(lines) == hint

        result = []
        if self._reader is None:
            self._reader = BlobStreamReader(self.client.download_blob())
        while len(line := self._reader.readline()) > 0:
            result.append(line)
            if _is_complete(result, hint):
                break
        return result

    def writelines(self, lines) -> None:
        raise NotImplementedError

    def __iter__(self) -> Iterator[bytes]:
        # TODO
        raise NotImplementedError


class BlobStreamReader:
    def __init__(self, stream) -> None:
        self.chunks = stream.chunks()
        self.leftover = bytearray()
        self.eof = False

    def readline(self) -> bytes:
        newline = b"\n"
        if self.eof:
            return EMPTY_BYTES
        while newline not in self.leftover:
            try:
                self.leftover.extend(next(self.chunks))
            except StopIteration:
                self.eof = True
                return bytes(self.leftover)
        curr, _, self.leftover = self.leftover.partition(newline)
        return bytes(curr + newline)

    def get_bytes(self, size: int) -> Bytes:
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
