package com.github.mmolimar.kukulcan

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

abstract class KukulcanApiTestHarness extends AnyWordSpecLike with Matchers {

  val KAFKA_PORT = 17000
  val ZOOKEEPER_PORT = 17001
  val SCHEMA_REGISTRY_PORT = 17002

  def apiClass: Class[_]

  def execJavaTests(): Unit

  def execScalaTests(): Unit

  s"a ${apiClass.getSimpleName}" when {
    "running tests for the Scala API" should {
      "validate its methods" in execScalaTests()
    }

    "running tests for the Java API" should {
      "validate its methods" in execJavaTests()
    }
  }

}
