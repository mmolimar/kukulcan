package com.github.mmolimar.kukulcan

import _root_.com.github.mmolimar.kukulcan.java.{KStreams => JKStreams}
import _root_.com.github.mmolimar.kukulcan.{KStreams => SKStreams}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}

import _root_.java.util.Properties

class KStreamsSpec extends KukulcanApiTestHarness {

  override def apiClass: Class[_] = classOf[SKStreams]

  override def execScalaTests(): Unit = {
    val scalaApi: SKStreams = {
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("client.id", "test-client")
      props.put("application.id", "test-app")
      props.put("replication.factor", "1")
      SKStreams(buildTopology, props)
    }

    scalaApi.getMetrics shouldBe scalaApi.getMetrics(".*", ".*")
    scalaApi.getMetrics("stream-metrics", "version").size shouldBe 1

    scalaApi.listMetrics()
    scalaApi.listMetrics(".*", ".*")
    scalaApi.printTopology()

    scalaApi.withApplicationId("test-app2") shouldNot be(None.orNull)
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("application.id", "test-app2")
    scalaApi.withProperties(props)
  }

  override def execJavaTests(): Unit = {
    val javaApi: JKStreams = {
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("client.id", "test-client")
      props.put("application.id", "test-app")
      props.put("replication.factor", "1")
      new JKStreams(buildTopology, props)
    }

    javaApi.getMetrics shouldBe javaApi.getMetrics(".*", ".*")
    javaApi.getMetrics("stream-metrics", "version").size shouldBe 1

    javaApi.listMetrics()
    javaApi.listMetrics(".*", ".*")
    javaApi.printTopology()

    javaApi.withApplicationId("test-app2") shouldNot be(None.orNull)
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("application.id", "test-app2")
    javaApi.withProperties(props)
  }

  private def buildTopology: Topology = {
    import org.apache.kafka.streams.scala.ImplicitConversions._

    implicit def Long: Serde[Long] = Serdes.longSerde

    implicit def String: Serde[String] = Serdes.stringSerde

    val builder = new StreamsBuilder()
    val global: GlobalKTable[Long, String] = builder.globalTable[Long, String](
      "sample",
      Materialized.as[Long, String, ByteArrayKeyValueStore]("test-global-table")
    )
    val records: KStream[Long, String] = builder.stream[Long, String]("test-stream")

    records
      .join(global)(
        (x, _) => x,
        (_, y) => y
      )
      .peek((_, _) => {})
    val wordCounts: KTable[String, Long] = records
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
      .groupBy((_, word) => word)
      .count()
    wordCounts.toStream.to("test-stream-result")

    builder.build()
  }

}
