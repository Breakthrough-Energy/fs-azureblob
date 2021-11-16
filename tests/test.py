import os
import unittest

import pytest
from fs.test import FSTestCases

from fs.azblob import BlobFS


@pytest.mark.skip
class TestBlobFS(FSTestCases, unittest.TestCase):
    def destroy_fs(self, fs):
        while any(fs.listdir("/")):
            fs.removetree("/")

    def make_fs(self):
        account_name = "besciences"
        container = "test3"
        key = os.getenv("BLOB_ACCOUNT_KEY")
        return BlobFS(account_name, container, key)
