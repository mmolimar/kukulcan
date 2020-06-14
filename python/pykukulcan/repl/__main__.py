import argparse
import atexit
import sys

from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JError

from pykukulcan.repl import PyKukulcanRepl

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PyKukulcan REPL.")
    parser.add_argument("--classpath", type=str, help="Classpath for the Kukulcan libs for the Java Gateway.")
    options = parser.parse_args()

    sys.ps1 = "@ "

    gateway = JavaGateway.launch_gateway(
        classpath=options.classpath,
        redirect_stdout=sys.stdout,
        redirect_stderr=sys.stderr)


    def close_gateway():
        gateway.shutdown()
        print("Bye!")


    atexit.register(close_gateway)

    try:
        gateway.jvm.com.github.mmolimar.kukulcan.repl.KukulcanRepl.printBanner()
        kukulcan = PyKukulcanRepl(gateway=gateway)
    except Py4JError:
        gateway.shutdown()
        sys.exit(1)
