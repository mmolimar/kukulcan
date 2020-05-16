package com.github.mmolimar.kukulcan

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

private[kukulcan] object KProducer extends Api[KProducer[AnyRef, AnyRef]]("producer") {

  override protected def createInstance(props: Properties): KProducer[AnyRef, AnyRef] = {
    KProducer[AnyRef, AnyRef](props)
  }

}

private[kukulcan] case class KProducer[K, V](private val props: Properties) extends KafkaProducer[K, V](props) {

  def reload(): Unit = KProducer.reload()

}
