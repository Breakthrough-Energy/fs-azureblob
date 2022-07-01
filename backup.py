import fs
import csv
import os
from fs.copy import copy_dir_if
from fs.wrap import read_only
import logging

logging.disable(logging.WARN)


account = "esmi"
container = "profiles"
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


def main():
    path = "data/input"
    fs1 = lfs.opendir(path)
    fs2 = bfs.opendir(path)
    copy_dir_if(fs1, ".", fs2, ".", condition="newer", on_copy=_on_copy)
