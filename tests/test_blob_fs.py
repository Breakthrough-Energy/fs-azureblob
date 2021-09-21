import io
import os

import pytest
from azure.storage.blob import BlobClient
from fs.mode import Mode

from fs import open_fs
from fs.azblob import BlobFile, BlobFS
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


@pytest.fixture
def account_key():
    key = os.getenv(BLOB_ACCOUNT_KEY)
    assert key is not None, f"{BLOB_ACCOUNT_KEY} required for write operations"
    return key


@pytest.fixture
def bfs_rw(account_key):
    return BlobFS(account_name, container, account_key=account_key)


def test_listdir(bfs):
    for path in ("", ".", "/", "raw", "raw/test_usa_tamu", "raw/test"):
        print(f"{path=}")
        print(bfs.listdir(path))


def test_getinfo(bfs):
    info = bfs.getinfo("version.json", namespaces=["details"])
    print(f"{info.name=}")
    print(f"{info.size=}")
    print(f"{info.created=}")
    print(f"{info.modified=}")


def test_download(bfs):
    fname = "demand_vJan2021.csv"
    path = f"raw/test_usa_tamu/{fname}"
    with open(fname, "wb") as f:
        bfs.download(path, f)

    assert os.path.exists(fname)
    assert os.stat(fname).st_size > 0
    os.remove(fname)


@pytest.mark.creds
def test_readline(bfs_rw):
    fname = "hello.txt"

    data = io.BytesIO(b"line1\nline2\n")
    bfs_rw.upload(fname, data)

    bc = BlobClient(url, container, fname)
    bfile = BlobFile(bc, Mode("r"))
    line1 = bfile.readline()
    assert line1 == b"line1\n"
    line2 = bfile.readline()
    assert line2 == b"line2\n"
    line3 = bfile.readline()
    assert line3 == b""

    bfs_rw.remove(fname)


class TestOpener:
    def test_opener(self):
        url = f"azblob://{account_name}@{container}"
        print(parse_fs_url(url))
        bfs = open_fs(url)
        assert isinstance(bfs, BlobFS)

    @pytest.mark.creds
    def test_opener_with_creds(self, account_key):
        url = f"azblob://{account_name}:{account_key}@{container}"
        print(parse_fs_url(url))
        bfs = open_fs(url)
        assert isinstance(bfs, BlobFS)


@pytest.mark.creds
def test_create(bfs_rw):
    fname = "hello.txt"
    data = io.BytesIO(b"hello")
    bfs_rw.upload(fname, data)

    assert fname in bfs_rw.listdir(".")

    bfs_rw.remove(fname)
    assert fname not in bfs_rw.listdir(".")
