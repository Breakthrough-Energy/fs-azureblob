from setuptools import setup

setup(
    name="fs-azureblob",
    author="Jon Hagg",
    author_email="jon@breakthroughenergy.org",
    description="An awesome filesystem for pyfilesystem2 !",
    install_requires=["fs~=2.4.13", "azure-storage-blob~=12.8.1"],
    entry_points={
        "fs.opener": [
            "azblob = fs.opener.blob_fs:BlobFSOpener",
        ]
    },
    license="MIT",
    packages=["fs.azblob", "fs.opener"],
    version="0.0.1",
)
