import io
from typing import Iterator, List

from azure.storage.blob import BlobClient


class BlobFile(io.IOBase):
    def __init__(self, client: BlobClient, mode=None):
        self._f = None
        self.client = client
        self.mode = mode

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
        return len(data)

    def read(self, n: int = -1) -> bytes:
        if n == -1:
            return self.readall()
        stream = self.client.download_blob()

        leftover = bytearray()
        chunks = iter(stream.chunks())
        while True:
            if len(leftover) < n:
                try:
                    leftover.extend(next(chunks))
                except StopIteration:
                    yield bytes(leftover)
            curr, leftover = leftover[:n], leftover[n:]
            if len(curr) == n:
                yield bytes(curr)

    def readall(self) -> bytes:
        stream = self.client.download_blob()
        return stream.content_as_bytes()

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
