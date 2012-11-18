import time
import unittest
from pycassa.cassandra.ttypes import ConsistencyLevel
from pycassa.columnfamily import ColumnFamily
from pycassa.pool import ConnectionPool
from pycassa.system_manager import SystemManager, SIMPLE_STRATEGY
from padlock.distributed.cassandra import CassandraDistributedRowLock


TEST_KS = '_TestCFDL_KS'
TEST_CF = 'CSDL'


class DistributedCassandraLockTestCase(unittest.TestCase):
    def setUp(self):
        self.sysman = SystemManager()
        self.sysman.create_keyspace(
            TEST_KS, SIMPLE_STRATEGY,
            dict(replication_factor='1')
        )
        self.sysman.create_column_family(TEST_KS, TEST_CF)

        self.pool = ConnectionPool(TEST_KS)

        self.cf = ColumnFamily(self.pool, TEST_CF)

    def tearDown(self):
        self.sysman.drop_keyspace(TEST_KS)
        del self.sysman

    def test_ttl(self):
        l = CassandraDistributedRowLock(
            self.pool, TEST_CF, "test_ttl",
            ttl=2.0,
            consistency_level=ConsistencyLevel.ONE,
            timeout=1.0
        )
        try:
            l.acquire()
            self.assertEqual(1, len(l.read_lock_columns()))
            time.sleep(3)
            self.assertEqual(0, len(l.read_lock_columns()))
        except:
            raise
        finally:
            l.release()
        self.assertEqual(0, len(l.read_lock_columns()))
