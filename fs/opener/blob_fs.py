__all__ = ["BlobFSOpener"]

from fs.azblob import BlobFS
from fs.azblob.blob_fs_v2 import BlobFSV2
from fs.opener import Opener
from fs.opener.errors import OpenerError


class BlobFSOpener(Opener):
    """Open url at azblob://<account_name>:<account_key>@<container>"""

    protocols = ["azblob"]

    def open_fs(self, fs_url, parse_result, writeable, create, cwd):
        account_name = parse_result.username
        account_key = parse_result.password
        container = parse_result.resource

        if account_name is None:
            raise OpenerError("account_name is required")
        if container is None:
            raise OpenerError("container is required")
        if account_key == "":
            account_key = None

        return BlobFS(account_name, container, account_key)


class BlobFSV2Opener(Opener):
    """Open url at azblobv2://<account_name>:<account_key>@<container>"""

    protocols = ["azblobv2"]

    def open_fs(self, fs_url, parse_result, writeable, create, cwd):
        account_name = parse_result.username
        account_key = parse_result.password
        container = parse_result.resource

        if account_name is None:
            raise OpenerError("account_name is required")
        if container is None:
            raise OpenerError("container is required")
        if account_key == "":
            account_key = None

        return BlobFSV2(account_name, container, account_key)
