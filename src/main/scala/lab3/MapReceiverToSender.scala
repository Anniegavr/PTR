package lab3
import akka.actor.ActorRef

class MapReceiverToSender(val prodReceiver: ActorRef, val prodSender: ActorRef)
