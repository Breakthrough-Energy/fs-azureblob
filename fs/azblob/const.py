# Placeholder for a directory. An empty blob with this name is created/removed when a
# directory is created/removed. It is excluded from list operations.
DIR_ENTRY = ".fs_azblob"

# getinfo namespaces
BASIC = "basic"
DETAILS = "details"
ACCESS = "access"
BLOB = "blob"

# getinfo keys
TYPE = "type"
IS_DIR = "is_dir"
NAME = "name"
ACCESSED = "accessed"
MODIFIED = "modified"
CREATED = "created"
METADATA_CHANGED = "metadata_changed"
SIZE = "size"
PERMISSIONS = "permissions"

# property names from azure sdk
LAST_ACCESSED_ON = "last_accessed_on"
CREATION_TIME = "creation_time"
LAST_MODIFIED = "last_modified"
METADATA = "metadata"


def _build_invalid_chars():
    ctrl_chars = "".join([chr(i) for i in range(32)])
    backslash = "\\"
    delete = chr(127)  # \x7F
    return ctrl_chars + backslash + delete


INVALID_CHARS = _build_invalid_chars()
READ_ONLY = "read_only"
