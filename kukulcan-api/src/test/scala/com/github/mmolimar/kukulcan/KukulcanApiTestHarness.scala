package com.github.mmolimar.kukulcan

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

abstract class KukulcanApiTestHarness extends AnyWordSpecLike with Matchers {

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
