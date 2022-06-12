package lab3

import akka.actor.ActorRef

import java.io.{BufferedReader, PrintStream}

case class ConsumersCommunication(val sender : ActorRef, val topic: String)
