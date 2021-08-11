__all__ = ["BlobFSOpener"]

from fs.opener import Opener
from fs.opener.errors import OpenerError

from src.blob_fs import BlobFS


class BlobFSOpener(Opener):
    """Open url at azblob://<account_name>:<account_key>@<container>"""

    protocols = ["azblob"]

    def open_fs(self, fs_url, parse_result, writeable, create, cwd):
        account_name = parse_result.username
        container = parse_result.resource

        if account_name is None:
            raise OpenerError("account_name is required")
        if container is None:
            raise OpenerError("container is required")

        return BlobFS(account_name, container)
