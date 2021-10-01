import datetime
from typing import BinaryIO

from azure.storage.blob import ContainerClient
from fs.base import FS
from fs.enums import ResourceType
from fs.errors import FSError, PermissionDenied, ResourceNotFound
from fs.info import Info
from fs.mode import Mode
from fs.path import basename, dirname
from fs.subfs import SubFS
from fs.time import datetime_to_epoch

from fs.azblob.blob_file import BlobFile


def _convert_to_epoch(props: dict) -> None:
    for k, v in props.items():
        if isinstance(v, datetime.datetime):
            props[k] = datetime_to_epoch(v)


def _basic_info(name: str, is_dir: bool) -> dict:
    return {"basic": {"name": name, "is_dir": is_dir}}


class BlobFS(FS):
    def __init__(self, account_name: str, container: str, account_key=None):
        super().__init__()
        self.client = ContainerClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            container_name=container,
            credential=account_key,
        )
        if not self.client.exists():
            raise FSError(
                f"The container: {container} does not exist. Please create it first"
            )

    def getinfo(self, path, namespaces=None) -> Info:
        namespaces = namespaces or ()
        path = self.validatepath(path)
        if path == "":
            # hack - so makedirs works as expected
            return Info(_basic_info("/", True))

        blob = self.client.get_blob_client(path)
        base_name = basename(path)
        if not blob.exists():
            if base_name not in self.listdir(dirname(path)):
                raise ResourceNotFound(path)
            return Info(_basic_info(name=base_name, is_dir=True))

        info = _basic_info(name=base_name, is_dir=False)

        if "details" in namespaces:
            props = blob.get_blob_properties()
            details = {}
            details["accessed"] = props["last_accessed_on"]
            details["created"] = props["creation_time"]
            details["metadata_changed"] = None
            details["modified"] = props["last_modified"]
            details["size"] = props["size"]
            details["type"] = ResourceType.file
            _convert_to_epoch(details)

            info["details"] = details
        return Info(info)

    def listdir(self, path) -> list:
        path = self.validatepath(path)
        parts = path.split("/")
        num_parts = 0 if path == "" else len(parts)
        suffix = parts[-1]
        _all = (b.name.split("/") for b in self.client.list_blobs(path))
        return list({p[num_parts] for p in _all if suffix in p or suffix == ""})

    def openbin(self, path, mode="r", buffering=-1, **options) -> BinaryIO:
        path = self.validatepath(path)
        mode = Mode(mode)
        return BlobFile(self.client.get_blob_client(path), mode)  # type: ignore

    def opendir(self, path, factory=None):
        return self.makedir(path)

    def validatepath(self, path: str) -> str:
        if path == ".":
            path = ""
        path = path.strip("/")
        return path

    def makedir(self, path, permissions=None, recreate=False) -> SubFS:  # type: ignore
        path = self.validatepath(path)
        return SubFS(self, path)

    def remove(self, path) -> None:
        path = self.validatepath(path)
        blob = self.client.get_blob_client(path)
        if not blob.exists():
            raise ResourceNotFound(path)
        self.client.delete_blob(path)

    def removedir(self, path) -> None:
        print("Directories not supported for azblob filesystem")

    def setinfo(self, path, info) -> None:
        raise PermissionDenied
