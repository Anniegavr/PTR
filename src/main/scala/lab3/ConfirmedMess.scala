package lab3

import akka.actor.ActorRef

case class ConfirmedMess(messageId : String, messageTopic: String)
