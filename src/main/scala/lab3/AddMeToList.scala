package lab3
import akka.actor.ActorRef

import java.io.BufferedReader

class AddMeToList(val selfRef : ActorRef, val producerSender: ActorRef, val message: Message, var sentTo: Int, var confirmedBy: Int)