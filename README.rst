padlock
=======

Padlock is a Python library that provides a lock through a single, simple interface and offers several backends
(actually, for now, only one, until someone `contributes another <http://github.com/samuraisam/padlock>`_) so
you can choose the backend that best fits your needs.

It's really easy to use. Here, for example, is how to create a cassandra row lock::

    >>> import padlock, pycassa
    >>> pool = pycassa.ConnectionPool('my_keyspace')
    >>> with padlock.get('cassandra, pool=pool, column_family='my_column_family'):
    ...    do_important_shit()
    "success!"

Huzzah!

Author: Samuel Sutch (`@ssutch <http://twitter.com/ssutch>`_)
PyPi: `http://pypi.python.org/pypi/padlock/ <http://pypi.python.org/pypi/padlock/>`_
Docs: `http://packages.python.org/padlock/ <http://packages.python.org/padlock/>`_
License: Apache 2.0

I am definitely open to contributions. Please feel free to submit your lock implementation.