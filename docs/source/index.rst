.. Padlock documentation master file, created by
   sphinx-quickstart on Sun Nov 18 10:03:59 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Padlock's documentation!
===================================

Padlock is a Python library that provides a lock through a single, simple interface and offers several backends
(actually, for now, only one, until someone `contributes another <http://github.com/samuraisam/padlock>`_) so
you can choose the backend that best fits your needs.

The Interface
-------------

.. automodule:: padlock

    .. py:function:: padlock.get(name, **kwargs)

        Returns a factory named by `name`. If `**kwargs` are provided, an instance will be constructed and
        the kwargs will be passed to the class desired.

        For instance, to construct a `cassandra` lock::

            >>> import padlock, pycassa
            >>> pool = pycassa.ConnectionPool('my_keyspace')
            >>> lock = padlock.get('cassandra', pool=pool, column_family='MyColumnFamily')
            >>> with lock:
            ...     do_some_stuff()
            "stuff"
            >>>

        :param name: The lock class desired (see :ref:`backends`)
        :type name: string
        :rtype: an :py:class:`ILock`

    .. autointerface:: ILock
        :members:
        :member-order: bysource

The Cassandra Backend
---------------------

.. automodule:: padlock.distributed.cassandra

.. autoclass:: StaleLockException

.. autoclass:: BusyLockException

.. autoclass:: CassandraDistributedRowLock
    :members:


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

