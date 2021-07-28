from azure.storage.blob import ContainerClient
from fs.base import FS


class BlobFS(FS):
    def __init__(self, account_name, container):
        # super().__init__()
        self.client = ContainerClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            container_name=container,
        )

    def getinfo(self):
        pass

    def listdir(self, path):
        if path == ".":
            path = ""
        return [b.name for b in self.client.list_blobs(path)]

    def makedir(self, path, permissions=None, recreate=False):
        pass

    def openbin(self, path, mode="r", buffering=-1, **options):
        pass

    def remove(self, path):
        pass

    def removedir(self, path):
        pass

    def setinfo(self, path, info):
        pass
