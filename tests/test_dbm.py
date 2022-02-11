import unittest

import pymongo
import pytest

from bzfunds.dbm import Manager


class TestDBM(unittest.TestCase):
    def setUp(self):
        self.dbm = Manager()

    def tearDown(self):
        pass

    def test_manager_connection(self):
        with pytest.raises(pymongo.errors.ServerSelectionTimeoutError):
            dbm = Manager("invalidhost", serverSelectionTimeoutMS=100)
            _ = dbm.client.list_databases()
