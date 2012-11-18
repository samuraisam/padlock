from zope.interface import Interface, implements


class IRetryPolicy(Interface):
    def duplicate(self):
        """
        Return a new instance of this class with the same startup properties but otherwise clean state"""

    def allow_retry(self):
        """Determines whether or not a retry should be allowed at this time. This may internally sleep..."""


class RunOncePolicy(object):
    """
    A RetryPolicy that runs only once
    """
    implements(IRetryPolicy)

    def duplicate(self):
        return self.__class__()

    def allow_retry(self):
        return False


