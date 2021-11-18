import datetime
import logging
from typing import Any, BinaryIO

from azure.storage.blob import ContainerClient
from fs.base import FS
from fs.enums import ResourceType
from fs.info import Info
from fs.mode import Mode
from fs.path import basename, dirname
from fs.subfs import SubFS
from fs.time import datetime_to_epoch

from fs import errors
from fs.azblob.blob_file import BlobFile
from fs.azblob.error_tools import blobfs_errors

logger = logging.getLogger(__name__)

DIR_ENTRY = ".fs_azblob"


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
        self._check_container_client()

    def _check_container_client(self):
        try:
            if self.client.exists():
                return
        except:  # noqa
            # if no credentials are provided, the check raises an auth error
            pass
        raise errors.FSError(
            "Invalid parameters. Either incorrect account details, or container does not exist"
        )

    def getinfo(self, path: str, namespaces=None) -> Info:
        self.check()
        namespaces = namespaces or ()
        path = self.validatepath(path)
        if path == "":
            # hack - so makedirs works as expected
            return Info(_basic_info("/", True))

        blob = self.client.get_blob_client(path)
        base_name = basename(path)
        if not blob.exists():
            if base_name not in self.listdir(dirname(path)):
                raise errors.ResourceNotFound(path)
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

    def listdir(self, path: str) -> list:
        self.check()
        path = self.validatepath(path)
        parts = path.split("/")
        num_parts = 0 if path == "" else len(parts)
        suffix = parts[-1]
        with blobfs_errors(path):
            _all = (b.name.split("/") for b in self.client.list_blobs(path))
            _all = (p[num_parts] for p in _all if suffix in p or suffix == "")
            return [a for a in _all if a != DIR_ENTRY]

    def openbin(
        self, path: str, mode: str = "r", buffering: int = -1, **options: Any
    ) -> BinaryIO:
        self.check()
        path = self.validatepath(path)
        _mode = Mode(mode)

        self._check_mode(path, _mode)
        # self._check_dir_path(path)
        return BlobFile(self.client.get_blob_client(path), _mode)  # type: ignore

    def _check_dir_path(self, path):
        try:
            dir_path = dirname(path)
            self.getinfo(dir_path)
        except errors.ResourceNotFound:
            raise errors.ResourceNotFound(path)

    def _check_mode(self, path, mode):
        mode.validate_bin()
        try:
            info = self.getinfo(path)
            if mode.exclusive:
                raise errors.FileExists(path)
            if info.is_dir:
                raise errors.FileExpected(path)
        except errors.ResourceNotFound:
            if not mode.create:
                raise errors.ResourceNotFound(path)

    def validatepath(self, path: str) -> str:
        if path == ".":
            path = ""
        return path.strip("/")

    def makedir(self, path: str, permissions=None, recreate: bool = False) -> SubFS:  # type: ignore
        self.check()
        path = self.validatepath(path)
        self.touch(path + "/" + DIR_ENTRY)
        return SubFS(self, path)

    def remove(self, path: str) -> None:
        self.check()
        path = self.validatepath(path)
        with blobfs_errors(path):
            self.client.delete_blob(path)

    def removedir(self, path: str) -> None:
        self.check()
        _path = self.validatepath(path)
        if _path == "":
            raise errors.RemoveRootError()
        info = self.getinfo(_path)
        if not info.is_dir:
            raise errors.DirectoryExpected(path)
        if not self.isempty(path):
            raise errors.DirectoryNotEmpty(path)
        self.remove(path + "/" + DIR_ENTRY)

    def setinfo(self, path: str, info) -> None:
        self.check()
        path = self.validatepath(path)
        if not self.exists(path):
            raise errors.ResourceNotFound(path)
        if "details" in info:
            details = info["details"]
            meta = {
                "last_accessed_on": str(details["accessed"]),
                "last_modified": str(details["modified"]),
            }
            with blobfs_errors(path):
                blob = self.client.get_blob_client(path)
                blob.set_blob_metadata(meta)
