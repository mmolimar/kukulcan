package com.github.mmolimar

import _root_.java.util.Properties

import com.github.mmolimar.kukulcan.repl._
import org.apache.kafka.streams.Topology

package object kukulcan {

  def admin: KAdmin = KAdminApi.inst

  def consumer[K, V]: KConsumer[K, V] = KConsumerApi.inst.asInstanceOf[KConsumer[K, V]]

  def producer[K, V]: KProducer[K, V] = KProducerApi.inst.asInstanceOf[KProducer[K, V]]

  def connect: KConnect = KConnectApi.inst

  def streams(topology: Topology): KStreams = KStreamsApi.inst(topology)

  def reload(): Unit = {
    KAdminApi.reload()
    KConsumerApi.reload()
    KProducerApi.reload()
    KConnectApi.reload()
    KStreamsApi.reload()
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

}
