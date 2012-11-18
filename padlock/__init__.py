from zope.interface import Interface


class ILock(Interface):
    def acquire(self):
        pass

    def release(self):
        pass

def load_zcml(package=None, name='configure.zcml'):
    pass