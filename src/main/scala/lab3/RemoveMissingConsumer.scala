package lab3
import akka.actor.ActorRef
case class RemoveMissingConsumer(consumerSenderId: String, consumerSenderRef: ActorRef)
