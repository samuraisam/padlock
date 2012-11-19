import sys
from zope.configuration.config import ConfigurationMachine
from zope.configuration import xmlconfig
from zope.interface import Interface
from zope.component import getUtility


class ILock(Interface):
    """
    Your average, run of the mill, generic lock interface.
    """
    def acquire(self):
        """
        Acquires the lock.
        """

    def release(self):
        """
        Releases the lock.
        """

    def __enter__(self):
        """
        :py:class:`ILock` is also a context manager, so you can use the convenient `with:` syntax
        """

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        The exit functionality for context managers (catches exceptions, releases the lock and whatnot)
        """

def load_zcml(package_name='padlock', spec='configure.zcml'):
    context = ConfigurationMachine()
    xmlconfig.registerCommonDirectives(context)
    __import__(package_name)
    package = sys.modules[package_name]
    xmlconfig.file(spec, package, context=context)

load_zcml()

def get(name):
    """
    Get a named lock class. You must provide initialization values to the lock class returned.
    """
    return getUtility(ILock, name)
