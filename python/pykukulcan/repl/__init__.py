__all__ = ['PyKukulcanRepl']


class PyKukulcanRepl:
    """
    Entry point for the PyKukulcan REPL.
    """

    _gateway = None

    def __init__(self, gateway):
        self._gateway = gateway

    def admin(self):
        """
        Create a KAdmin instance reading the {@code admin.properties} file.
        If the instance was already created, it will be reused.

        :return: The KAdmin instance initialized.
        """
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.admin()

    def consumer(self):
        """
        Create a KConsumer instance reading the {@code consumer.properties} file.
        If the instance was already created, it will be reused.

        :return: The KConsumer instance initialized.
        """
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.consumer()

    def producer(self):
        """
        Create a KProducer instance reading the {@code producer.properties} file.
        If the instance was already created, it will be reused.

        :return: The KProducer instance initialized.
        """
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.producer()

    def connect(self):
        """
        Create a KConnect instance reading the {@code connect.properties} file.
        If the instance was already created, it will be reused.

        :return: The KConnect instance initialized.
        """
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.connect()

    def streams(self, topology):
        """
        Create a KStreams instance reading the {@code streams.properties} file.
        If the instance was already created, it will be reused.

        :return: The KStreams instance initialized.
        """
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.streams(topology)

    def schema_registry(self):
        """
        Create a KSchemaRegistry instance reading the {@code schema-registry.properties} file.
        If the instance was already created, it will be reused.

        :return: The KSchemaRegistry instance initialized.
        """
        return self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.schemaRegistry()

    def reload(self):
        """
        Re-create all instances using their properties files.
        """
        self._gateway.jvm.com.github.mmolimar.kukulcan.java.Kukulcan.reload()
