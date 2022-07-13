from typing import Any, BinaryIO

from azure.storage.filedatalake import DataLakeServiceClient
from fs.base import FS
from fs.info import Info
from fs.mode import Mode
from fs.path import basename, dirname
from fs.permissions import Permissions
from fs.subfs import SubFS

from fs import errors
from fs.azblob.blob_file import BlobFile
from fs.azblob.const import (
    ACCESS,
    ACCESSED,
    BASIC,
    BLOB,
    CREATED,
    CREATION_TIME,
    DETAILS,
    INVALID_CHARS,
    IS_DIR,
    LAST_MODIFIED,
    METADATA,
    METADATA_CHANGED,
    MODIFIED,
    NAME,
    PERMISSIONS,
    READ_ONLY,
    SIZE,
)
from fs.azblob.error_tools import blobfs_errors
from fs.azblob.helpers import _convert_to_epoch, _info_from_dict


def _is_dir(blob):
    metadata = blob["metadata"]
    if metadata is not None:
        if "hdi_isfolder" in metadata:
            if metadata["hdi_isfolder"] == "true":
                return True
    return False


def _basic_info(props) -> dict:
    return {BASIC: {NAME: props[NAME], IS_DIR: _is_dir(props)}}


class BlobFSV2(FS):
    _meta = {"invalid_path_chars": INVALID_CHARS}

    def __init__(self, account_name: str, container: str, account_key=None):
        super().__init__()
        self._svc = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=account_key,
        )
        self.client = self._svc.get_file_system_client(container)
        self._check_container_client()
        self._meta = self._meta.copy()
        self._meta[READ_ONLY] = account_key is None

    def _check_container_client(self):
        try:
            if not self.client.exists():
                raise errors.CreateFailed("Container does not exist")
        except:  # noqa
            raise errors.CreateFailed(
                "Invalid parameters. Either incorrect account details, or container does not exist"
            )

    def getinfo(self, path: str, namespaces=None) -> Info:
        self.check()
        namespaces = namespaces or ()
        _path = self.validatepath(path)

        blob = self.client.get_file_client(_path)
        if not blob.exists():
            raise errors.ResourceNotFound(path)

        props = dict(blob.get_file_properties())
        props[NAME] = basename(_path)
        info = _basic_info(props)
        if info[BASIC][IS_DIR]:
            return _info_from_dict(info, namespaces)

        if DETAILS in namespaces:
            details = {
                ACCESSED: None,
                CREATED: props[CREATION_TIME],
                METADATA_CHANGED: None,
                MODIFIED: props[LAST_MODIFIED],
                SIZE: props[SIZE],
            }
            _convert_to_epoch(details)
            info[DETAILS] = details

        if BLOB in namespaces:
            info[BLOB] = props[METADATA]

        if ACCESS in namespaces:
            perms = blob.get_access_control()[PERMISSIONS]
            info[ACCESS] = {
                PERMISSIONS: Permissions(
                    user=perms[:3], group=perms[3:6], other=perms[6:]
                )
            }

        return _info_from_dict(info, namespaces)

    def listdir(self, path: str) -> list:
        self.check()
        path = self.validatepath(path)
        if not self.getinfo(path).is_dir:
            raise errors.DirectoryExpected(path)
        paths = self.client.get_paths(path, recursive=False)
        return [p["name"].split("/")[-1] for p in paths]

    def openbin(
        self, path: str, mode: str = "r", buffering: int = -1, **options: Any
    ) -> BinaryIO:
        self.check()
        path = self.validatepath(path)
        _mode = Mode(mode)

        self._check_mode(path, _mode)
        self._check_dir_path(path)
        blob = self.client.get_file_client(path)
        blob_file = BlobFile.factory(blob, _mode.to_platform_bin(), version=2)

        if not blob.exists():
            blob.create_file()
        elif _mode.truncate:
            blob.delete_file()
            blob.create_file()

        stream = blob.download_file()
        stream.readinto(blob_file.raw)

        if not _mode.appending:
            blob_file.seek(0)

        return blob_file  # type: ignore

    def _check_dir_path(self, path: str) -> None:
        """Ensure the parent directory of path exists"""
        try:
            dir_path = dirname(path)
            self.getinfo(dir_path)
        except errors.ResourceNotFound:
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

    def _check_makedir(self, path: str, recreate: bool) -> None:
        if not self.isdir(dirname(path)):
            raise errors.ResourceNotFound(path)
        if not recreate:
            if path == "" or self.exists(path):
                raise errors.DirectoryExists(path)

    def makedir(self, path: str, permissions=None, recreate: bool = False) -> SubFS:  # type: ignore
        self.check()
        path = self.validatepath(path)
        self._check_makedir(path, recreate)
        dir_client = self.client.get_directory_client(path)
        if not dir_client.exists():
            dir_client.create_directory()
        return SubFS(self, path)

    def move(
        self,
        src_path: str,
        dst_path: str,
        overwrite: bool = False,
        preserve_time: bool = False,
    ) -> None:

        self.check()
        if not overwrite and self.exists(dst_path):
            raise errors.DestinationExists(dst_path)
        if self.isdir(src_path):
            raise errors.FileExpected(src_path)

        src_path = self.validatepath(src_path)
        dst_path = self.validatepath(dst_path)

        with blobfs_errors(src_path):
            blob = self.client.get_file_client(src_path)
            with blobfs_errors(dst_path):
                blob.rename_file(self.client.file_system_name + dst_path)

    def remove(self, path: str) -> None:
        self.check()
        _path = self.validatepath(path)
        if self.getinfo(path).is_dir:
            raise errors.FileExpected(path)
        with blobfs_errors(path):
            self.client.delete_file(_path)

    def removedir(self, path: str) -> None:
        self.check()
        path = self.validatepath(path)
        if path == "/":
            raise errors.RemoveRootError()
        info = self.getinfo(path)
        if not info.is_dir:
            raise errors.DirectoryExpected(path)
        if not self.isempty(path):
            raise errors.DirectoryNotEmpty(path)
        self.client.delete_directory(path)

    def setinfo(self, path: str, info) -> None:
        self.check()
        path = self.validatepath(path)
        if not self.exists(path):
            raise errors.ResourceNotFound(path)
        with blobfs_errors(path):
            blob = self.client.get_file_client(path)
            meta = blob.get_file_properties()[METADATA]
            meta.update(info.get(BLOB, {}))
            blob.set_metadata(meta)
