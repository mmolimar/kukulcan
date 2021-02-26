package com.github.mmolimar.kukulcan.repl

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class KukulcanReplSpec extends AnyWordSpecLike with Matchers {

  s"a Kukulcan Scala REPL" when {

    "initializing" should {
      val interpreter = new KukulcanILoop()
      interpreter.settings = KukulcanRepl.buildSettings
      interpreter.createInterpreter()

      "have an interpreter" in {
        interpreter.intp shouldNot be(None.orNull)
        interpreter.printWelcome()
        interpreter.prompt shouldBe "@ "
      }
    }

    "using the KApi" should {

      "reload services" in {
        import com.github.mmolimar.kukulcan
        import org.apache.kafka.streams.scala.ImplicitConversions._

        implicit def String: Serde[String] = Serdes.stringSerde

        val admin = kukulcan.admin
        val connect = kukulcan.connect
        val consumer = kukulcan.consumer
        val producer = kukulcan.producer
        val topology = {
          val streamsBuilder = new StreamsBuilder()
          streamsBuilder.stream[String, String]("test")
          streamsBuilder.build()
        }
        val streams = {
          kukulcan.streams(topology)
        }
        val ksql = kukulcan.ksqldb
        val schemaRegistry = kukulcan.schemaRegistry

        admin shouldBe kukulcan.admin
        connect shouldBe kukulcan.connect
        consumer shouldBe kukulcan.consumer
        producer shouldBe kukulcan.producer
        streams shouldNot be(kukulcan.streams(topology))
        ksql shouldBe kukulcan.ksqldb
        schemaRegistry shouldBe kukulcan.schemaRegistry

        kukulcan.reload()

        admin shouldNot be(kukulcan.admin)
        connect shouldNot be(kukulcan.connect)
        consumer shouldNot be(kukulcan.consumer)
        producer shouldNot be(kukulcan.producer)
        streams shouldNot be(kukulcan.streams(topology))
        ksql shouldNot be(kukulcan.ksqldb)
        schemaRegistry shouldNot be(kukulcan.schemaRegistry)
      }
    }
  }

  s"a Kukulcan Java REPL" when {
    "initializing" should {

      "have default args" in {
        val args = JKukulcanRepl.shellArgs(Array.empty)
        args.contains("--feedback") shouldBe true
        args.contains("concise") shouldBe true
      }
    }

    "using the KApi" should {

      "reload services" in {
        import com.github.mmolimar.kukulcan.java._
        import org.apache.kafka.streams.scala.ImplicitConversions._

        implicit def String: Serde[String] = Serdes.stringSerde

        val admin = Kukulcan.admin
        val connect = Kukulcan.connect
        val consumer = Kukulcan.consumer
        val producer = Kukulcan.producer
        val topology = {
          val streamsBuilder = new StreamsBuilder()
          streamsBuilder.stream[String, String]("test")
          streamsBuilder.build()
        }
        val streams = {
          Kukulcan.streams(topology)
        }
        val ksql = Kukulcan.ksqldb
        val schemaRegistry = Kukulcan.schemaRegistry

        admin shouldBe Kukulcan.admin
        connect shouldBe Kukulcan.connect
        consumer shouldBe Kukulcan.consumer
        producer shouldBe Kukulcan.producer
        streams shouldNot be(Kukulcan.streams(topology))
        ksql shouldBe Kukulcan.ksqldb
        schemaRegistry shouldBe Kukulcan.schemaRegistry

        Kukulcan.reload()

        admin shouldNot be(Kukulcan.admin)
        connect shouldNot be(Kukulcan.connect)
        consumer shouldNot be(Kukulcan.consumer)
        producer shouldNot be(Kukulcan.producer)
        streams shouldNot be(Kukulcan.streams(topology))
        ksql shouldNot be(Kukulcan.ksqldb)
        schemaRegistry shouldNot be(Kukulcan.schemaRegistry)
      }
    }
  }
}
