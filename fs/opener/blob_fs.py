__all__ = ["BlobFSOpener", "BlobFSV2Opener"]

from fs.azblob import BlobFS
from fs.azblob.blob_fs_v2 import BlobFSV2
from fs.opener import Opener
from fs.opener.errors import OpenerError


def open_fs(fs_class, parse_result):
    account_name = parse_result.username
    account_key = parse_result.password
    container = parse_result.resource

    if account_name is None:
        raise OpenerError("account_name is required")
    if container is None:
        raise OpenerError("container is required")
    if account_key == "":
        account_key = None

    return fs_class(account_name, container, account_key)


class BlobFSOpener(Opener):
    """Open url at azblob://<account_name>:<account_key>@<container>"""

    protocols = ["azblob"]

    def open_fs(self, fs_url, parse_result, writeable, create, cwd):
        return open_fs(BlobFS, parse_result)


class BlobFSV2Opener(Opener):
    """Open url at azblobv2://<account_name>:<account_key>@<container>"""

    protocols = ["azblobv2"]

    def open_fs(self, fs_url, parse_result, writeable, create, cwd):
        return open_fs(BlobFSV2, parse_result)
