import fs
import os
from fs.copy import copy_dir_if, copy_file
from fs.wrap import read_only
import logging

logging.disable(logging.WARN)


account = "esmi"
container = "scenariodata"
key = os.environ["BLOB_ACCOUNT_KEY_V2"]


bfs = fs.open_fs(f"azblobv2://{account}:{key}@{container}")
lfs = read_only(fs.open_fs("/mnt/bes/pcm"))


def _on_copy(src_fs, src_path, dst_fs, dst_path):
    msg = f"{src_path}\n"
    print(msg)
    with open("migration.log", "a") as f:
        f.write(msg)


def _size(_fs, path):
    if not _fs.exists(path):
        return None
    try:
        return _fs.getsize(path)
    except Exception as e:
        print(e)
        return -1


class Backup:
    def __init__(self, path):
        self.path = path
        self.fs1 = lfs.opendir(path)
        self.fs2 = bfs.opendir(path)

    def _mismatch(self):
        result = []
        for _path in self.fs1.walk.files():
            s1 = _size(self.fs1, _path)
            s2 = _size(self.fs2, _path)
            if s2 is None or s1 != s2:
                print(f"{_path=}, {s1=}, {s2=}")
                result.append(_path)
        return result

    def fix_missing(self):
        for p in self._mismatch():
            copy_file(self.fs1, p, self.fs2, p)

    def main(self):
        copy_dir_if(self.fs1, ".", self.fs2, ".", condition="newer", on_copy=_on_copy)


if __name__ == "__main__":
    path = "data/output"
    backup = Backup(path)
    # backup.fix_missing()
    backup.main()
