package lab3

import java.io.{BufferedReader, PrintStream}

case class ConsumersCommunication(val os : PrintStream, val is: BufferedReader, val topic: String)
