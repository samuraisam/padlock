from setuptools import setup

setup(
    name="padlock",
    version="0.0.2",
    url='http://github.com/samuraisam/padlock',
    author='Samuel Sutch',
    author_email='samuel.sutch@gmail.com',
    description='A lock library with multiple backends',
    long_description=
"""
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
""",
    packages=['padlock'],
    install_requires=[
        'zope.interface==4.0.1',
        'zope.component==4.0.0',
        'zope.configuration==4.0.0',
        'time_uuid==0.1.0'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ]
)
