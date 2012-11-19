"""
The Cassandra lock is a lock implemented in a cassandra row. It's best used when also using cassandra for some other
purpose and contention would likely not be an issue anyway (the lock provides the extra safety gear necessary to
perform some operation).

To use the `cassandra` lock, instantiate using the :py:func:`padlock.get` function. You'll also need a recent
version of `pycassa <http://github.com/pycassa/pycassa>`_ installed::

    import padlock, pycassa
    pool = pycassa.ConnectionPool('my_keyspsace'
    with padlock.get('cassandra', pool=pool, column_family='my_column_family'):
        do_some_important_shit()

Success! Please read through the class documentation to get a feel of how to use the lock. It will likely be a little
specific to your implementation.
"""

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
    """
    Raised when a lock is already taken on this row.
    """


class StaleLockException(Exception):
    """
    Raised old, stale locks exist on a row and we don't want them to have existed.
    """

# This is pretty much directly lifted from the excellent Astynax cassandra clibrary from Netflix
#
# Here is their copyright:
#
#    Copyright 2011 Netflix
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

class CassandraDistributedRowLock(object):
    """
    A lock that is implemented in a row of a Cassandra column family. It's good to use this type of lock when you want
    to lock a single row in cassandra for some purpose in a scenario where there will not be a lot of lock contention.

    Shamelessly lifted from: Netflix's `Astynax library <https://github.com/Netflix/astyanax>`_. Take a `look <https://github.com/Netflix/astyanax/blob/master/src/main/java/com/netflix/astyanax/recipes/locks/ColumnPrefixDistributedRowLock.java>`_ at the implementation (in Java).

    :param pool: A pycassa ConnectionPool. It will be used to facilitate communication with cassandra.
    :type pool: pycassa.pool.ConnectionPool
    :param column_family: Either a `string` (which will then be made into a `pycassa.column_family.ColumnFamily` instance) or
        an already configured instance of `ColumnFamily` (which will be used directly).
    :type column_family: string
    :param key: The row key for this lock. The lock can co-exist with other columns on an existing row if desired.
    :type key: string

    The following paramters are optional and all come with defaults:

    :param prefix: The column prefix. Defaults to `_lock_`
    :type prefix: str
    :param lock_id: A unique string, should probably be a UUIDv1 if provided at all. Defaults to a UUIDv1 provided by `time-uuid <http://github.com/samuraisam/time_uuid>`_
    :type lock_id: str
    :param fail_on_stale_lock: Whether or not to fail when stale locks are found. Otherwise they'll just be cleaned up.
    :type fail_on_stale_lock: bool
    :param timeout: How long to wait until the lock is considered stale. You should set this to as much time as you think the work will take using the lock.
    :type timeout: float
    :param ttl: How many seconds until cassandra will automatically clean up stale locks. It must be greater than `timeout`.
    :type ttl: float
    :param backoff_policy: a :py:class:`padlock.distributed.retry_policy.IRetryPolicy` instance. Governs the retry policy of acquiring the lock.
    :type backoff_policy: IRetryPolicy
    :param allow_retry: Whether or not to allow retry. Defaults to `True`
    :type allow_retry: bool

    You can also provide the following keyword arguments which will be passed directly to the `ColumnFamily` constructor
    if you didn't provide the instance yourself:

        * **read_consistency_level**
        * **write_consistency_level**
        * **autopack_names**
        * **autopack_values**
        * **autopack_keys**
        * **column_class_name**
        * **super_column_name_class**
        * **default_validation_class**
        * **column_validators**
        * **key_validation_class**
        * **dict_class**
        * **buffer_size**
        * **column_bufer_size**
        * **timestamp**
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
        self.timeout = kwargs.get('timeout', 60.0)  # seconds
        self.ttl = kwargs.get('ttl', None)
        self.backoff_policy = kwargs.get('backoff_policy', getUtility(IRetryPolicy, 'run_once'))
        self.allow_retry = kwargs.get('allow_retry', True)
        self.locks_to_delete = set()
        self.lock_column = None

    def acquire(self):
        """
        Acquire the lock on this row. It will then read immediatly from cassandra, potentially retrying, potentially
        sleeping the executing thread.
        """
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

    def release(self):
        """
        Allow this row to be locked by something (or someone) else. Performs a single write (round trip) to Cassandra.
        """
        if not len(self.locks_to_delete) or self.lock_column is not None:
            mutation = self.column_family.batch()
            self.fill_release_mutation(mutation, False)
            mutation.send()

    def verify_lock(self, cur_time):
        """
        Whether or not the lock can be verified by reading the row and ensuring the paramters of the lock
        according to the current :py:class:`CassandraDistributedRowLock` instance's configuration is valid.

        This must only be called after :py:meth:`acquire` is called, or else you will get a :py:class:`ValueError`

        :param cur_time: The current time in microseconds
        :type cur_time: long
        :rtype: None
        """
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

    def read_lock_columns(self):
        """
        Return all columns in this row with the timeout value deserialized into a long

        :rtype: dict
        """
        res = {}
        try:
            cols = self.column_family.get(self.key, column_count=1e9)
        except NotFoundException:
            cols = {}
        for k, v in cols.iteritems():
            res[k] = self.read_timeout_value(v)
        return res

    def release_locks(self, force=False):
        """
        Clean up after ourselves. Removes all lock columns (everything returned by :py:meth:`read_lock_columns`)
        that is not stale.

        :param force: Remove even non-stale locks
        """
        locks = self.read_lock_columns()
        cols_to_remove = []
        now = self.utcnow()
        for k, v in locks.iteritems():
            if force or (v > 0 and v < now):
                cols_to_remove.add(k)

        self.column_family.batch().remove(self.key, cols_to_remove).send()

        return locks

    def utcnow(self):
        """
        Used internally - return the current time, as microseconds from the unix epoch (Jan 1 1970 UTC)

        :rtype: long
        """
        d = datetime.datetime.utcnow()
        return long(calendar.timegm(d.timetuple())*1e6) + long(d.microsecond)

    def fill_lock_mutation(self, mutation, time, ttl):
        """
        Used internally - fills out `pycassa.batch.CfMutator` with the necessary steps to acquire the lock.
        """
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
        """
        Used internally - serialize a timeout value (a `long`) to be inserted into a cassandra as a `string`.
        """
        return repr(timeout_val)

    def read_timeout_value(self, col):
        """
        Used internally - deserialize a timeout value that was stored in cassandra as a `string` back into a `long`.
        """
        return long(col)

    def fill_release_mutation(self, mutation, exclude_current_lock=False):
        """
        Used internally - used to fill out a `pycassa.batch.CfMutator` with the necessary steps to release the lock.
        """
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
