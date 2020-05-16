package com.github.mmolimar.kukulcan

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

private[kukulcan] object KConsumer extends Api[KConsumer[AnyRef, AnyRef]]("consumer") {

  override protected def createInstance(props: Properties): KConsumer[AnyRef, AnyRef] = {
    KConsumer[AnyRef, AnyRef](props)
  }

}

private[kukulcan] case class KConsumer[K, V](private val props: Properties) extends KafkaConsumer[K, V](props) {

  def reload(): Unit = KConsumer.reload()

}
