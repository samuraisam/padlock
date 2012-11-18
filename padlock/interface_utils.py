from zope.component import getGlobalSiteManager

def utility(iface, name):
    def dec(klass):
        site = getGlobalSiteManager()
        site.registerUtility(klass, iface, name=name)
        return klass
    return dec
