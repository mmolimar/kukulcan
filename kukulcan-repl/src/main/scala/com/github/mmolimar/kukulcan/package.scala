package com.github.mmolimar

import _root_.java.util.Properties

import com.github.mmolimar.kukulcan.repl._
import org.apache.kafka.streams.Topology

package object kukulcan {

  /**
   * Create a KAdmin instance reading the {@code admin.properties} file.
   * If the instance was already created, it will be reused.
   *
   * @return The KAdmin instance initialized.
   */
  def admin: KAdmin = KAdminApi.inst

  /**
   * Create a KConsumer instance reading the {@code consumer.properties} file.
   * If the instance was already created, it will be reused.
   *
   * @return The KConsumer instance initialized.
   */
  def consumer[K, V]: KConsumer[K, V] = KConsumerApi.inst.asInstanceOf[KConsumer[K, V]]

  /**
   * Create a KProducer instance reading the {@code producer.properties} file.
   * If the instance was already created, it will be reused.
   *
   * @return The KProducer instance initialized.
   */
  def producer[K, V]: KProducer[K, V] = KProducerApi.inst.asInstanceOf[KProducer[K, V]]

  /**
   * Create a KConnect instance reading the {@code connect.properties} file.
   * If the instance was already created, it will be reused.
   *
   * @return The KConnect instance initialized.
   */
  def connect: KConnect = KConnectApi.inst

  /**
   * Create a KStreams instance reading the {@code streams.properties} file.
   * If the instance was already created, it will be reused.
   *
   * @param topology The topology to create the KStream
   * @return The KStreams instance initialized.
   */
  def streams(topology: Topology): KStreams = KStreamsApi.inst(topology)

  /**
   * Create a KSchemaRegistry instance reading the {@code schema-registry.properties} file.
   * If the instance was already created, it will be reused.
   *
   * @return The KSchemaRegistry instance initialized.
   */
  def schemaRegistry: KSchemaRegistry = KSchemaRegistryApi.inst

  /**
   * Re-create all instances using their properties files.
   *
   */
  def reload(): Unit = {
    KAdminApi.reload()
    KConsumerApi.reload()
    KProducerApi.reload()
    KConnectApi.reload()
    KStreamsApi.reload()
    KSchemaRegistryApi.reload()
    println("Done!")
  }

  private[kukulcan] object KAdminApi extends KApi[KAdmin]("admin") {
    override protected def createInstance(props: Properties): KAdmin = KAdmin(props)
  }

  private[kukulcan] object KConsumerApi extends KApi[KConsumer[AnyRef, AnyRef]]("consumer") {
    override protected def createInstance(props: Properties): KConsumer[AnyRef, AnyRef] = KConsumer(props)
  }

  private[kukulcan] object KProducerApi extends KApi[KProducer[AnyRef, AnyRef]]("producer") {
    override protected def createInstance(props: Properties): KProducer[AnyRef, AnyRef] = KProducer(props)
  }

  private[kukulcan] object KConnectApi extends KApi[KConnect]("connect") {
    override protected def createInstance(props: Properties): KConnect = KConnect(props)
  }

  private[kukulcan] object KStreamsApi extends KApi[Topology => KStreams]("streams") {
    override protected def createInstance(props: Properties): Topology => KStreams = {
      topology: Topology => KStreams(topology, props)
    }
  }

  private[kukulcan] object KSchemaRegistryApi extends KApi[KSchemaRegistry]("schema-registry") {
    override protected def createInstance(props: Properties): KSchemaRegistry = KSchemaRegistry(props)
  }

}
