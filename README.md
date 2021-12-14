[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![Tests](https://github.com/Breakthrough-Energy/fs-azureblob/workflows/Tests/badge.svg)
![GitHub contributors](https://img.shields.io/github/contributors/Breakthrough-Energy/fs-azureblob?logo=GitHub)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/Breakthrough-Energy/fs-azureblob?logo=GitHub)
![GitHub last commit (branch)](https://img.shields.io/github/last-commit/Breakthrough-Energy/fs-azureblob/main?logo=GitHub)
![GitHub pull requests](https://img.shields.io/github/issues-pr/Breakthrough-Energy/fs-azureblob?logo=GitHub)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code of Conduct](https://img.shields.io/badge/code%20of-conduct-ff69b4.svg?style=flat)](https://breakthrough-energy.github.io/docs/communication/code_of_conduct.html)

# fs-azureblob

## Installation

The package can be installed via pip:
```
pip install git+https://github.com/Breakthrough-Energy/fs-azureblob
```

Or by cloning the repository and installing directly:
```
git clone https://github.com/Breakthrough-Energy/fs-azureblob
cd fs-azureblob
pip install .
```
Either approach will also install the core `fs` package if it's not already installed.

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

## Note
Since blob storage uses a flat namespace (directories don't really exist), we create a
placeholder file to represent them, always named `.fs_azblob`. This is an empty blob
which is created for new directories, removed when a directory is removed, and omitted
from `listdir` results, so should be transparent to users. To use this package on a new
blob storage container, nothing needs to be done. For usage on an existing container,
one should create this structure using the azure portal, sdk, or preferred tool, to
ensure this package will function as expected.

Additionally, this package is intended to operate on "block blobs". Other blob types
include page blobs and append blobs. The package has not been tested on these types.


## See also

-   [fs](https://github.com/Pyfilesystem/pyfilesystem2), the core
    PyFilesystem2 library
