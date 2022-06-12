package lab3

import akka.actor.ActorRef

import java.io.{BufferedReader, PrintStream}

/**
 * Class representing info about
 * @param sender - the broker's sender actor dedicated to a consumer
 * @param topic - and the topic the consumer waits for
 */
case class ConsumersCommunication(val sender : ActorRef, val topic: String)
