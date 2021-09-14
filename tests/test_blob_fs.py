import io
import os

import pytest

from fs.azblob import BlobFS
from fs.opener.blob_fs import BlobFSOpener

BLOB_ACCOUNT_KEY = "BLOB_ACCOUNT_KEY"
account_name = "besciences"
container = "test"


@pytest.fixture
def bfs():
    return BlobFS(account_name, container)


@pytest.fixture
def bfs_rw():
    key = os.getenv(BLOB_ACCOUNT_KEY)
    assert key is not None, f"{BLOB_ACCOUNT_KEY} required for write operations"
    return BlobFS(account_name, container, account_key=key)


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


def test_opener():
    from fs.opener.parse import parse_fs_url
    from fs import open_fs
    from fs.opener.registry import registry

    registry.install(BlobFSOpener)

    url = f"azblob://{account_name}@{container}"
    print(parse_fs_url(url))
    bfs = open_fs(url)
    assert isinstance(bfs, BlobFS)


@pytest.mark.skip
def test_create(bfs_rw):
    fname = "hello.txt"
    data = io.BytesIO(b"hello")
    bfs_rw.upload(fname, data)

    assert fname in bfs_rw.listdir(".")

    bfs_rw.remove(fname)
    assert fname not in bfs_rw.listdir(".")
