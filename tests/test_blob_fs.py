import io
import os
from contextlib import contextmanager
from uuid import uuid4

import pytest

from fs import errors, open_fs
from fs.azblob import BlobFS
from fs.opener.blob_fs import BlobFSOpener
from fs.opener.parse import parse_fs_url
from fs.opener.registry import registry

registry.install(BlobFSOpener)

BLOB_ACCOUNT_KEY = "BLOB_ACCOUNT_KEY"
account_name = "besciences"
container = "test"
url = f"https://{account_name}.blob.core.windows.net"


@pytest.fixture
def bfs():
    return BlobFS(account_name, container)


ACCOUNT_KEY = os.getenv(BLOB_ACCOUNT_KEY)


@pytest.fixture
def bfs_rw():
    return BlobFS(account_name, container, account_key=ACCOUNT_KEY)


@contextmanager
def new_file(bfs_rw, content=b""):
    fname = "hello.txt"
    if bfs_rw.exists(fname):
        bfs_rw.remove(fname)
    bfs_rw.upload(fname, io.BytesIO(content))
    yield fname
    bfs_rw.remove(fname)


def test_listdir(bfs):
    for path in ("", ".", "/", "raw", "raw/test_usa_tamu"):
        print(f"{path=}")
        print(bfs.listdir(path))


def test_getinfo(bfs):
    info = bfs.getinfo("version.json", namespaces=["details"])
    print(f"{info.name=}")
    print(f"{info.size=}")
    print(f"{info.created=}")
    print(f"{info.modified=}")
    assert info.size > 0


def test_download(bfs):
    fname = "demand_vJan2021.csv"
    path = f"raw/test_usa_tamu/{fname}"
    with open(fname, "wb") as f:
        bfs.download(path, f)

    assert os.path.exists(fname)
    assert os.stat(fname).st_size > 0
    os.remove(fname)


def test_download_not_exists(bfs):
    fname = str(uuid4())
    with pytest.raises(errors.ResourceNotFound):
        bfs.download(fname, io.BytesIO())


@pytest.mark.creds
def test_remove_not_found(bfs_rw):
    with pytest.raises(errors.ResourceNotFound):
        bfs_rw.remove(str(uuid4()))


@pytest.mark.creds
def test_makedirs(bfs_rw):
    path = "some/path"
    sub_fs = bfs_rw.makedirs(path)
    with new_file(sub_fs, b"whatever") as fname:
        list1 = sub_fs.listdir(".")
        list2 = bfs_rw.listdir(path)
        assert list1 == list2 == [fname]
    bfs_rw.removetree(path)


@pytest.mark.creds
class TestBlobFile:
    def test_readline(self, bfs_rw):
        with new_file(bfs_rw, b"line1\nline2\n") as fname:
            with bfs_rw.openbin(fname) as bfile:
                line = bfile.readline()
                assert line == b"line1\n"
                line = bfile.readline()
                assert line == b"line2\n"
                line = bfile.readline()
                assert line == b""

    def test_readline_with_limit(self, bfs_rw):
        with new_file(bfs_rw, b"line1\nline2") as fname:
            with bfs_rw.openbin(fname) as bfile:
                line = bfile.readline(3)
                assert line == b"lin"
                line = bfile.readline(4)
                assert line == b"e1\n"
                line = bfile.readline()
                assert line == b"line2"

    def test_open_close(self, bfs_rw):
        with new_file(bfs_rw) as fname:
            with bfs_rw.openbin(fname) as bfile:
                assert not bfile.closed
            assert bfile.closed

    def test_iterate_lines(self, bfs_rw):
        with new_file(bfs_rw, b"line1\nline2\n\n") as fname:
            with bfs_rw.openbin(fname) as bfile:
                assert 3 == sum(1 for _ in bfile)

    def test_readall(self, bfs_rw):
        with new_file(bfs_rw, b"line1\nline2\n\n") as fname:
            with bfs_rw.openbin(fname) as bfile:
                line1 = bfile.readline()
                assert b"line1\n" == line1
                remaining = bfile.readall()
                assert line1 not in remaining
                assert b"line2" in remaining


class TestOpener:
    def test_opener(self):
        url = f"azblob://{account_name}@{container}"
        print(parse_fs_url(url))
        bfs = open_fs(url)
        assert isinstance(bfs, BlobFS)

    @pytest.mark.creds
    def test_opener_with_creds(self):
        url = f"azblob://{account_name}:{ACCOUNT_KEY}@{container}"
        print(parse_fs_url(url))
        bfs = open_fs(url)
        assert isinstance(bfs, BlobFS)

    def test_opener_error_wrong_container(self):
        url = f"azblob://{account_name}@{str(uuid4())}"
        with pytest.raises(errors.CreateFailed):
            open_fs(url)

    def test_opener_error_wrong_key(self):
        url = f"azblob://{account_name}:asdf@{container}"
        with pytest.raises(errors.CreateFailed):
            open_fs(url)


@pytest.mark.creds
class TestUpload:
    def test_create(self, bfs_rw):
        with new_file(bfs_rw, b"hello") as fname:
            assert fname in bfs_rw.listdir(".")
        assert fname not in bfs_rw.listdir(".")

    def test_upload_empty(self, bfs_rw):
        with new_file(bfs_rw, b"") as fname:
            assert fname in bfs_rw.listdir(".")

    def test_upload_overwrite(self, bfs_rw):
        fname = "duplicate.txt"
        bfs_rw.upload(fname, io.BytesIO(b"foo"))
        bfs_rw.upload(fname, io.BytesIO(b"bar"))
        assert b"bar" == bfs_rw.readbytes(fname)
        bfs_rw.remove(fname)

    @pytest.mark.skip
    def test_upload_large_file(self, bfs_rw):
        # creates 95 MB file
        fname = "big.txt"
        data = io.BytesIO(b"many_bytes" * int(1e7))
        bfs_rw.upload(fname, data)
        assert fname in bfs_rw.listdir(".")
        bfs_rw.remove(fname)
