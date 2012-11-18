import calendar
import datetime
from zope.interface import  implements
from zope.component import getUtility
from time_uuid import TimeUUID
from padlock import ILock
from padlock.distributed.retry_policy import IRetryPolicy

try:
    from pycassa import ConsistencyLevel, ColumnFamily, NotFoundException
except ImportError:
    # pycassa must be available for any of this to work
    ConsistencyLevel = ColumnFamily = NotFoundException = None


# args that we'll read from the keyword arguments and pass to the column family constructor
_cf_args = [
    'read_consistency_level',
    'write_consistency_level',
    'autopack_names',
    'autopack_values',
    'autopack_keys',
    'column_class_name',
    'super_column_name_class',
    'default_validation_class',
    'column_validators',
    'key_validation_class',
    'dict_class',
    'buffer_size',
    'column_bufer_size',
    'timestamp'
]


class BusyLockException(Exception):
    pass


class StaleLockException(Exception):
    pass


class CassandraDistributedRowLock(object):
    """
    A lock that is implemented in the row of a Cassandra column family.

    Shamelessly lifted from: Netflix's Astynax library:

    https://github.com/Netflix/astyanax/blob/master/src/main/java/com/netflix/astyanax/recipes/locks/ColumnPrefixDistributedRowLock.java

    Here is their license agreement:

    Copyright 2011 Netflix

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
    """

    implements(ILock)

    def __init__(self, pool, column_family, key, **kwargs):
        self.pool = pool
        if isinstance(column_family, ColumnFamily):
            self.column_family = column_family
        else:
            cf_kwargs = {k: kwargs.get(k) for k in _cf_args if k in kwargs}
            self.column_family = ColumnFamily(self.pool, column_family, **cf_kwargs)
        self.key = key
        self.consistency_level = kwargs.get('consistency_level', ConsistencyLevel.LOCAL_QUORUM)
        self.prefix = kwargs.get('prefix', '_lock_')
        self.lock_id = kwargs.get('lock_id', str(TimeUUID.with_utcnow()))
        self.fail_on_stale_lock = kwargs.get('fail_on_stale_lock', False)
        self.lock_column = kwargs.get('lock_column', None)
        self.timeout = kwargs.get('timeout', 60.0)  # seconds
        self.ttl = kwargs.get('ttl', None)
        self.backoff_policy = kwargs.get('backoff_policy', getUtility(IRetryPolicy, 'run_once'))
        self.allow_retry = kwargs.get('allow_retry', True)
        self.locks_to_delete = set()

    def acquire(self):
        if self.ttl is not None:
            if self.timeout > self.ttl:
                raise ValueError("Timeout {} must be less than TTL {}".format(self.timeout, self.ttl))

        retry = self.backoff_policy.duplicate()
        retry_count = 0

        while True:
            try:
                cur_time = self.utcnow()

                mutation = self.column_family.batch()
                self.fill_lock_mutation(mutation, cur_time, self.ttl)
                mutation.send()

                self.verify_lock(cur_time)

                self.acquire_time = self.utcnow()

                return
            except BusyLockException, e:
                self.release()
                if not retry.allow_retry():
                    raise e
                retry_count += 1

    def verify_lock(self, cur_time):
        if self.lock_column is None:
            raise ValueError("verify_lock() called without attempting to take the lock")

        cols = self.read_lock_columns()
        for k, v in cols.iteritems():
            if v != 0 and cur_time > v:
                if self.fail_on_stale_lock:
                    raise StaleLockException("Stale lock on row '{}'. Manual cleanup required.".format(self.key))
                self.locks_to_delete.add(k)
            elif k != self.lock_column:
                raise BusyLockException("Lock already acquired for row '{}' with lock column '{}'".format(self.key, k))

    def release(self):
        if not len(self.locks_to_delete) or self.lock_column is not None:
            mutation = self.column_family.batch()
            self.fill_release_mutation(mutation, False)
            mutation.send()

    def read_lock_columns(self):
        res = {}
        try:
            cols = self.column_family.get(self.key, column_count=1e9)
        except NotFoundException:
            cols = {}
        for k, v in cols.iteritems():
            res[k] = self.read_timeout_value(v)
        return res

    def release_locks(self, force=False):
        locks = self.read_lock_columns()
        cols_to_remove = []
        now = self.utcnow()
        for k, v in locks.iteritems():
            if force or (v > 0 and v < now):
                cols_to_remove.add(k)

        self.column_family.batch().remove(self.key, cols_to_remove).send()

        return locks

    def utcnow(self):
        d = datetime.datetime.utcnow()
        return long(calendar.timegm(d.timetuple())*1e6) + long(d.microsecond)

    def fill_lock_mutation(self, mutation, time, ttl):
        if self.lock_column is not None:
            if self.lock_column != (self.prefix + self.lock_id):
                raise ValueError("Can't change prefix or lock_id after acquiring the lock")
        else:
            self.lock_column = self.prefix + self.lock_id
        if time is None:
            timeout_val = 0
        else:
            timeout_val = time + long(self.timeout * 1e6) # convert self.timeout to microseconds

        kw = {}
        if ttl is not None:
            kw['ttl'] = ttl

        mutation.insert(self.key, {self.lock_column: self.generate_timeout_value(timeout_val)}, **kw)

        return self.lock_column

    def generate_timeout_value(self, timeout_val):
        return repr(timeout_val)

    def read_timeout_value(self, col):
        return long(col)

    def fill_release_mutation(self, mutation, exclude_current_lock=False):
        cols_to_delete = []

        for lock_col_name in self.locks_to_delete:
            cols_to_delete.append(lock_col_name)

        if not exclude_current_lock and self.lock_column is not None:
            cols_to_delete.append(self.lock_column)

        mutation.remove(self.key, cols_to_delete)

        self.locks_to_delete.clear()
        self.lock_column = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
