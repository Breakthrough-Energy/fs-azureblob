import os

import pytest

from src.blob_fs import BlobFS

account_name = "besciences"
container = "profiles"


@pytest.mark.skip
def test_listdir():
    bfs = BlobFS(account_name, container)
    for path in ("", ".", "/", "raw", "raw/usa_tamu", "raw_usa"):
        print(f"{path=}")
        print(bfs.listdir(path))


@pytest.mark.skip
def test_getinfo():
    bfs = BlobFS(account_name, container)
    info = bfs.getinfo("version.json", namespaces=["details"])
    print(f"{info.name=}")
    print(f"{info.size=}")
    print(f"{info.created=}")
    print(f"{info.modified=}")


def test_download():
    bfs = BlobFS(account_name, container)
    fname = "demand_vJan2021.csv"
    path = f"raw/usa_tamu/{fname}"
    with open(fname, "wb") as f:
        bfs.download(path, f)

    assert os.path.exists(fname)
    os.remove(fname)
