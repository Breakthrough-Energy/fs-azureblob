import os
import unittest

import pytest
from fs.test import FSTestCases

from fs.azblob import BlobFS


@pytest.mark.creds
class TestBlobFS(FSTestCases, unittest.TestCase):
    def make_fs(self):
        account_name = "besciences"
        container = "test3"
        key = os.getenv("BLOB_ACCOUNT_KEY")
        bfs = BlobFS(account_name, container, key)
        bfs.removetree("/")
        return bfs
