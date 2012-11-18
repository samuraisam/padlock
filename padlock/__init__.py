from zope.interface import Interface


class ILock(Interface):
    def acquire(self):
        pass

    def release(self):
        pass

