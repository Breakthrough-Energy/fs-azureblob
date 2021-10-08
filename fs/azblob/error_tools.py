import contextlib
import traceback

from azure.core.exceptions import (
    ClientAuthenticationError,
    ResourceExistsError,
    ResourceNotFoundError,
)

from fs import errors


@contextlib.contextmanager
def blobfs_errors(path):
    try:
        yield
    except ClientAuthenticationError:
        raise errors.PermissionDenied
    except ResourceExistsError:
        raise errors.DestinationExists(path)
    except ResourceNotFoundError:
        raise errors.ResourceNotFound(path)
    except:  # noqa
        raise errors.FSError(traceback.format_exc())
