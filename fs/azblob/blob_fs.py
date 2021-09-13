import datetime
import io

from azure.storage.blob import ContainerClient
from fs.base import FS
from fs.enums import ResourceType
from fs.errors import PermissionDenied, ResourceNotFound
from fs.info import Info
from fs.mode import Mode
from fs.path import basename
from fs.subfs import SubFS
from fs.time import datetime_to_epoch

from fs.azblob.blob_file import BlobFile


def _convert_to_epoch(props: dict) -> None:
    for k, v in props.items():
        if isinstance(v, datetime.datetime):
            props[k] = datetime_to_epoch(v)


class BlobFS(FS):
    def __init__(self, account_name: str, container: str, account_key=None):
        super().__init__()
        self.client = ContainerClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            container_name=container,
            credential=account_key,
        )

    def getinfo(self, path, namespaces=None) -> Info:
        namespaces = namespaces or ()
        path = self.validatepath(path)
        blob = self.client.get_blob_client(path)
        if not blob.exists():
            raise ResourceNotFound(path)
        info = {"basic": {"name": basename(path), "is_dir": False}}
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
        all = (b.name.split("/") for b in self.client.list_blobs(path))
        return list({p[num_parts] for p in all if suffix in p or suffix == ""})

    def openbin(self, path, mode="r", buffering=-1, **options) -> io.IOBase:
        path = self.validatepath(path)
        mode = Mode(mode)
        if mode.reading:
            return BlobFile(self.client.get_blob_client(path), mode)

        elif mode.writing:
            raise ValueError("writing mode not supported on openbin yet")

    def validatepath(self, path: str) -> str:
        if path == ".":
            path = ""
        path = path.strip("/")
        return path

    def makedir(self, path, permissions=None, recreate=False) -> SubFS:
        print("Directories not supported for azblob filesystem")

    def remove(self, path) -> None:
        path = self.validatepath(path)
        self.client.delete_blob(path)

    def removedir(self, path) -> None:
        print("Directories not supported for azblob filesystem")

    def setinfo(self, path, info) -> None:
        raise PermissionDenied
