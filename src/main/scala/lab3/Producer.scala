package lab3

import akka.actor.{Actor, ActorSystem, Props}

import java.io.{BufferedReader, ByteArrayOutputStream, InputStreamReader, ObjectOutputStream, PrintStream}
import java.net.{ServerSocket, Socket}
import java.nio.charset.StandardCharsets
import java.util.{Base64, UUID}

case object SendMess

class MessageSender(os: PrintStream) extends Actor{
  def receive = {
    case SendMess =>

      val producerValueType = List[String]("troopers", "Yoda", "Mandalorians")
      val chosenTopic = scala.util.Random.between(0,2)
      val priority  = scala.util.Random.between(0,3)
      val valueOfMessage = scala.util.Random.between(0,200)
      val msg = new Message(UUID.randomUUID().toString, priority, producerValueType(chosenTopic), valueOfMessage)
      println("priority " + msg.priority + " | "+ msg.topic + " " + msg.value)
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(msg)
      oos.close()
      val retv = new String(
        Base64.getEncoder().encode(stream.toByteArray),
        StandardCharsets.UTF_8
      )
      os.println(retv)
      Thread.sleep(10000)
      self ! SendMess
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
  messageSender ! SendMess
}
