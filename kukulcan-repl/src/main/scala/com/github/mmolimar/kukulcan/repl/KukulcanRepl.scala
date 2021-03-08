package com.github.mmolimar.kukulcan.repl

import scala.tools.nsc.{GenericRunnerSettings, Settings}
import scala.tools.nsc.interpreter.ILoop

/**
 * Entry point for the Kukulcan Scala REPL.
 *
 */
object KukulcanRepl extends App {

  def printBanner(): Unit = println(banner)

  def buildSettings: Settings = {
    val settings = new GenericRunnerSettings(Console.err.println)
    settings.usejavacp.value = true
    settings.processArguments(Option(args).map(_.toList).getOrElse(List.empty), processAll = true)

    settings
  }

  new KukulcanILoop().process(buildSettings)
}

private[repl] class KukulcanILoop extends ILoop {

  private val initCommands =
    """
      |import com.github.mmolimar.kukulcan
      |""".stripMargin

  override def printWelcome(): Unit = KukulcanRepl.printBanner()

  override def prompt = "@ "

  override def createInterpreter(): Unit = {
    super.createInterpreter()
    intp.beQuietDuring {
      intp.interpret(initCommands)
    }
  }

}
