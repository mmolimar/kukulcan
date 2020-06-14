__all__ = ['PyKukulcanRepl']


class PyKukulcanRepl:
    _gateway = None

    def __init__(self, gateway):
        self._gateway = gateway

    def admin(self):
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.admin()

    def consumer(self):
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.consumer()

    def producer(self):
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.producer()

    def connect(self):
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.connect()

    def streams(self, topology):
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.streams(topology)

    def reload(self):
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.reload()
