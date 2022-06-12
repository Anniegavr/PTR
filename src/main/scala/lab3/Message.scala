package lab3
import java.io.Serializable

/**
 * Used as message (command)
 * @param id - the current message unique id
 * @param priority - the priority in the queue (not implemented yet)
 * @param topic - the topic this message belongs to
 * @param value - dummy info that comes on the topic
 */
class Message(val id:String, val priority: Int, val topic: String, val value:Int) extends Serializable
