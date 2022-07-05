import fs
import csv
import os
from fs.copy import copy_dir_if
from fs.wrap import read_only
import logging

logging.disable(logging.WARN)


account = "esmi"
container = "scenariodata"
key = os.environ["BLOB_ACCOUNT_KEY_V2"]


lfs = read_only(fs.open_fs("/mnt/bes/pcm"))
bfs = fs.open_fs(f"azblobv2://{account}:{key}@{container}")


def list_ids(_fs):
    path = "ScenarioList.csv"
    ids = []
    with _fs.open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            ids.append(row["id"])
    return ids


def _on_copy(src_fs, src_path, dst_fs, dst_path):
    msg = f"{src_path}\n"
    print(msg)
    with open("migration.log", "a") as f:
        f.write(msg)


def _size(_fs, path):
    if not _fs.exists(path):
        return None
    try:
        return _fs.getinfo(path, namespaces=["details"]).size
    except Exception as e:
        print(e)
        return -1


def mismatch(path):
    fs1 = lfs.opendir(path)
    fs2 = bfs.opendir(path)
    for _path in fs1.walk.files():
        s1 = _size(fs1, _path)
        s2 = _size(fs2, _path)
        if s2 is None or s1 != s2:
            print(f"{_path=}, {s1=}, {s2=}")


def main():
    path = "data/output"
    fs1 = lfs.opendir(path)
    fs2 = bfs.opendir(path)
    copy_dir_if(fs1, ".", fs2, ".", condition="newer", on_copy=_on_copy)


if __name__ == "__main__":
    mismatch("data/input")
    # mismatch("data/output")
