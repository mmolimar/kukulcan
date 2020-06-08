package com.github.mmolimar.kukulcan.repl

import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.ILoop

object KukulcanRepl extends App {

  def printBanner(): Unit = println(banner)

  val settings = new GenericRunnerSettings(Console.err.println)
  settings.usejavacp.value = true
  settings.processArguments(args.toList, processAll = true)

  new KukulcanILoop().process(settings)
}

private class KukulcanILoop extends ILoop {

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
