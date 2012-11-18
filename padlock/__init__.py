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
        Acquires the lock
        """

    def release(self):
        """
        Releases the lock
        """

    def __enter__(self):
        """
        ILock is a context manager
        """

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        ILock is a context manager
        """


def load_zcml(package_name='padlock', spec='configure.zcml'):
    context = ConfigurationMachine()
    xmlconfig.registerCommonDirectives(context)
    __import__(package_name)
    package = sys.modules[package_name]
    xmlconfig.file(spec, package, context=context)

load_zcml()

def get(name):
    return getUtility(ILock, name)
