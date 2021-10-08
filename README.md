# fs-azureblob


## Usage

### Opener

Use `fs.open_fs` to open a filesystem with an azure blob
[FS URL](https://docs.pyfilesystem.org/en/latest/openers.html):

```python
import fs
my_fs = fs.open_fs("azblob://[account_name]:[account_key]@[container]")
```

### Constructor

The `BlobFS` class can also be instantiated directly

```python
from fs.azblob import BlobFS
my_fs = BlobFS(account_name, container, account_key)
```

using the following arguments:

- `account_name`: the name of the storage account
- `container`: the blob container
- `account_key`: optional, but required for write operations or depending on the storage account access policies


## See also

-   [fs](https://github.com/Pyfilesystem/pyfilesystem2), the core
    PyFilesystem2 library
