import io

import requests
from azure.storage.blob import ContainerClient

from fs.base import FS
from fs.info import Info
from fs.mode import Mode
from fs.path import basename
from fs.subfs import SubFS


class BlobFS(FS):
    def __init__(self, account_name, container):
        # super().__init__()
        self.client = ContainerClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            container_name=container,
        )

    def getinfo(self, path, namespaces=None) -> Info:
        info = {"basic": {"name": basename(path), "is_dir": False}}
        return Info(info)

    def listdir(self, path) -> list:
        if path == ".":
            path = ""
        return [b.name for b in self.client.list_blobs(path)]

    def makedir(self, path, permissions=None, recreate=False) -> SubFS:
        pass

    def openbin(self, path, mode="r", buffering=-1, **options) -> io.IOBase:
        self.check()
        _path = self.validatepath(path)
        _mode = Mode(mode)
        _mode.validate_bin()

        with requests.get(_path, stream=True) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=8192):
                yield chunk

    def remove(self, path) -> None:
        pass

    def removedir(self, path) -> None:
        pass

    def setinfo(self, path, info) -> None:
        pass
