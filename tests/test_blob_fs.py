import os

import pytest

from fs.azblob import BlobFS
from fs.opener.blob_fs import BlobFSOpener

account_name = "besciences"
container = "profiles"


def test_listdir():
    bfs = BlobFS(account_name, container)
    for path in ("", ".", "/", "raw", "raw/usa_tamu", "raw_usa"):
        print(f"{path=}")
        print(bfs.listdir(path))


def test_getinfo():
    bfs = BlobFS(account_name, container)
    info = bfs.getinfo("version.json", namespaces=["details"])
    print(f"{info.name=}")
    print(f"{info.size=}")
    print(f"{info.created=}")
    print(f"{info.modified=}")


@pytest.mark.skip
def test_download():
    bfs = BlobFS(account_name, container)
    fname = "demand_vJan2021.csv"
    path = f"raw/usa_tamu/{fname}"
    with open(fname, "wb") as f:
        bfs.download(path, f)

    assert os.path.exists(fname)
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
