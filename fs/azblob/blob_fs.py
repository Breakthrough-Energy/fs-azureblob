import datetime
from typing import Any, BinaryIO

from azure.storage.blob import ContainerClient
from fs.base import FS
from fs.enums import ResourceType
from fs.info import Info
from fs.mode import Mode
from fs.path import basename, dirname, join
from fs.subfs import SubFS
from fs.time import datetime_to_epoch

from fs import errors
from fs.azblob.blob_file import BlobFile
from fs.azblob.const import (
    ACCESSED,
    BASIC,
    CREATED,
    CREATION_TIME,
    DETAILS,
    DIR_ENTRY,
    INVALID_CHARS,
    IS_DIR,
    LAST_ACCESSED_ON,
    LAST_MODIFIED,
    METADATA_CHANGED,
    MODIFIED,
    NAME,
    SIZE,
    TYPE,
)
from fs.azblob.error_tools import blobfs_errors


def _convert_to_epoch(props: dict) -> None:
    for k, v in props.items():
        if isinstance(v, datetime.datetime):
            props[k] = datetime_to_epoch(v)


def _basic_info(name: str, is_dir: bool) -> dict:
    return {BASIC: {NAME: name, IS_DIR: is_dir}}


def _info_from_dict(info: dict, namespaces) -> Info:
    if DETAILS in namespaces:
        if DETAILS not in info:
            info[DETAILS] = {}
        if info[BASIC][IS_DIR]:
            info[DETAILS][TYPE] = ResourceType.directory
        else:
            info[DETAILS][TYPE] = ResourceType.file
    return Info(info)


class BlobFS(FS):
    _meta = {"invalid_path_chars": INVALID_CHARS}

    def __init__(self, account_name: str, container: str, account_key=None):
        super().__init__()
        self.client = ContainerClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            container_name=container,
            credential=account_key,
        )
        self._check_container_client()
        self._init()

    def _check_container_client(self):
        try:
            if not self.client.exists():
                raise errors.CreateFailed("Container does not exist")
        except:  # noqa
            raise errors.CreateFailed(
                "Invalid parameters. Either incorrect account details, or container does not exist"
            )

    def _init(self):
        root = self.client.get_blob_client(DIR_ENTRY)
        if not root.exists():
            root.upload_blob(b"")

    def getinfo(self, path: str, namespaces=None) -> Info:
        self.check()
        namespaces = namespaces or ()
        path = self._validatepath(path)
        base_name = basename(path)

        dir_blob = self.client.get_blob_client(join(path, DIR_ENTRY))
        if dir_blob.exists():
            info = _basic_info(base_name, is_dir=True)
            return _info_from_dict(info, namespaces)

        blob = self.client.get_blob_client(path)
        if not blob.exists():
            raise errors.ResourceNotFound(path)

        info = _basic_info(name=base_name, is_dir=False)
        if DETAILS in namespaces:
            props = blob.get_blob_properties()
            details = {
                ACCESSED: props[LAST_ACCESSED_ON],
                CREATED: props[CREATION_TIME],
                METADATA_CHANGED: None,
                MODIFIED: props[LAST_MODIFIED],
                SIZE: props[SIZE],
            }
            _convert_to_epoch(details)
            info[DETAILS] = details

        return _info_from_dict(info, namespaces)

    def _list_blob_names(self, path: str) -> list:
        """List blobs relative to a given path. For example, if the blob
        foo/bar/file.txt exists, and the given path is foo/bar, this will return a
        list containing 'file.txt'
        """
        parts = path.split("/")
        num_parts = 0 if path == "" else len(parts)
        suffix = parts[-1]
        with blobfs_errors(path):
            _all = [b.name.split("/") for b in self.client.list_blobs(path)]
            _all = [p[num_parts] for p in _all if suffix in p or suffix == ""]
            return list(set(_all))

    def listdir(self, path: str) -> list:
        self.check()
        path = self._validatepath(path)
        if not self.getinfo(path).is_dir:
            raise errors.DirectoryExpected(path)
        return [b for b in self._list_blob_names(path) if b != DIR_ENTRY]

    def openbin(
        self, path: str, mode: str = "r", buffering: int = -1, **options: Any
    ) -> BinaryIO:
        self.check()
        path = self._validatepath(path)
        _mode = Mode(mode)

        self._check_mode(path, _mode)
        self._check_dir_path(path)
        blob = self.client.get_blob_client(path)
        blob_file = BlobFile.factory(blob, _mode.to_platform_bin())

        if self.exists(path):
            stream = blob.download_blob()
            stream.readinto(blob_file.raw)

        if _mode.truncate:
            blob_file.seek(0)
            blob_file.truncate()
        elif not _mode.appending:
            blob_file.seek(0)

        return blob_file  # type: ignore

    def _check_dir_path(self, path: str) -> None:
        """Ensure the parent directory of path exists"""
        try:
            dir_path = dirname(path)
            self.getinfo(dir_path)
        except errors.ResourceNotFound:
            if DIR_ENTRY != basename(path):
                raise errors.ResourceNotFound(path)

    def _check_mode(self, path: str, mode: Mode) -> None:
        """Ensure path can be opened using the given mode"""
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

    def _validatepath(self, path: str) -> str:
        _path = super().validatepath(path)
        if _path.endswith("."):
            raise errors.InvalidPath(path)

        return _path.strip("/")

    def _check_makedir(self, path: str, recreate: bool) -> None:
        if not self.isdir(dirname(path)):
            raise errors.ResourceNotFound(path)
        if not recreate:
            if path == "" or self.exists(path):
                raise errors.DirectoryExists(path)

    def makedir(self, path: str, permissions=None, recreate: bool = False) -> SubFS:  # type: ignore
        self.check()
        path = self._validatepath(path)
        self._check_makedir(path, recreate)
        self.touch(path + "/" + DIR_ENTRY)
        return SubFS(self, path)

    def remove(self, path: str) -> None:
        self.check()
        path = self._validatepath(path)
        if self.getinfo(path).is_dir:
            raise errors.FileExpected(path)
        with blobfs_errors(path):
            self.client.delete_blob(path)

    def removedir(self, path: str) -> None:
        self.check()
        _path = self._validatepath(path)
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
        path = self._validatepath(path)
        if not self.exists(path):
            raise errors.ResourceNotFound(path)
        if DETAILS in info:
            details = info[DETAILS]
            meta = {
                LAST_ACCESSED_ON: str(details[ACCESSED]),
                LAST_MODIFIED: str(details[MODIFIED]),
            }
            with blobfs_errors(path):
                blob = self.client.get_blob_client(path)
                blob.set_blob_metadata(meta)
