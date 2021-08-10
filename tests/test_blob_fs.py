from src.blob_fs import BlobFS


account_name = "besciences"
container = "profiles"


def test_listdir():
    bfs = BlobFS(account_name, container)
    for path in ("", ".", "/", "raw", "raw/usa_tamu", "raw_usa"):
        print(f"{path=}")
        print(bfs.listdir(path))
