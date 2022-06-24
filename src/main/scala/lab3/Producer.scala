package lab3

import akka.actor.{Actor, ActorSystem, Props}
import lab3.Consumer.{consumerSystem, is, ps}

import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader, ObjectInputStream, ObjectOutputStream, PrintStream}
import java.net.{ServerSocket, Socket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{Base64, UUID}

case object SendMess
//case object receiveMess
case object quit

/**
 * Actor that produces messages to be sent to the broker
 * @param os - output stream through which the actor publishes a message to the broker on the socket connection
 */
class MessageSender(os: PrintStream) extends Actor{
  var numberMsgesWithoutConfirmation = 0
  override def postStop(): Unit = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(quit)
    oos.close()
    val retv = new String(
      Base64.getEncoder().encode(stream.toByteArray),
      StandardCharsets.UTF_8
    )
    os.println(retv)
  }
  def receive = {
//    case confirmedMess: ConfirmedMess
    case SendMess =>
      val producerValueType = List[String]("troopers", "Yoda", "Mandalorians")
      

      val chosenTopic = scala.util.Random.between(0,2)
      val priority  = scala.util.Random.between(0,3)
      val valueOfMessage = scala.util.Random.between(0,200)
      val msg = new Message(UUID.randomUUID().toString.substring(0, 4), priority, producerValueType(chosenTopic), valueOfMessage)
      println("id"+msg.id+",priority " + msg.priority + " topic "+ msg.topic + " value " + msg.value)
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(msg)
      oos.close()
      val retv = new String(
        Base64.getEncoder().encode(stream.toByteArray),
        StandardCharsets.UTF_8
      )
      os.println(retv)
      numberMsgesWithoutConfirmation +=1
      Thread.sleep(500)
      self ! SendMess
  }
}
/**
 * Class to receive messages from the broker
 * @param is - the input stream
 * @param ps - the output stream
 */
class ProducerMessagesReceive(is: BufferedReader) extends Actor
{
//  val receivedMessages = new ConcurrentLinkedQueue[Message]()

  override def preStart(): Unit = {
    println("Producer receiver "+ self)
  }
  override def postStop(): Unit = {
    println("Producer receiver: I've stopped :( ")
  }
  def receive: Receive = {
    case receiveMess =>
      println("Prod rcvr: listening cycles on")
      if(is.ready){
        val input = is.readLine
        println("Rcvr: got confirmation")
        val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case confirmedMess: ConfirmedMess =>
            println("Producer received confirmation, id" + confirmedMess.messageId)
          case _ => throw new Exception("Dis is not a message from client")
        }
        ois.close()
//        println("Ack for message "+msgo.messageId)
      }
      Thread.sleep(500)
      self ! receiveMess
  }
}

object Producer  extends App{
  val host = "localhost"
  val port = 4444
  val sock = new Socket(host, port)
  val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
  val os = new PrintStream(sock.getOutputStream)
  val producerSystem = ActorSystem("derProducer")
  os.println("producer")
  val messageSender = producerSystem.actorOf(Props(classOf[MessageSender], os), "sendMessagesManagerName")
  val producerMessageReceiver = producerSystem.actorOf(Props(classOf[ProducerMessagesReceive], is), "producerMessagesReceive")
  messageSender ! SendMess
  producerMessageReceiver ! receiveMess
}
