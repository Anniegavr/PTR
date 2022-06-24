package lab3

import akka.actor.{Actor, ActorSystem, Props}
import lab3.Consumer.{consumerSystem, is, ps}
import lab3.Producer.{os, producerSystem}

import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, FileNotFoundException, InputStreamReader, ObjectInputStream, ObjectOutputStream, PrintStream}
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.{Base64, Properties, UUID}
import scala.io.Source

case object receiveMess
case object confirmMess

/**
 * Class to receive messages from the broker
 * @param is - the input stream
 * @param ps - the output stream
 */
class ConsumerMessagesReceive(is: BufferedReader, ps: PrintStream) extends Actor {
  override def preStart(): Unit = {
    println("Consumer " + self)
  }

  override def postStop(): Unit = {
    println("Consumer: I've stopped :( ")
  }

  def receive: Receive = {
    case receiveMess =>
      if (is.ready) {
        println("received!")
        val input = is.readLine
        val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        val msgo = ois.readObject match {
          case msg: Message => msg
          case _ => throw new Exception("Dis is not a message from client")
        }
        ois.close()

        println("Received: " + msgo.topic + " " + msgo.value + "| priority " + msgo.priority)

        //        val msg2 = new Confirm(msgo.id, msgo.topic)
        //        println("Sending confirmation:" + msgo.id)
        //        val stream2: ByteArrayOutputStream = new ByteArrayOutputStream()
        //        val oos2 = new ObjectOutputStream(stream2)
        //        oos2.writeObject(msg2)
        //        oos2.close()
        //        val retv2 = new String(
        //          Base64.getEncoder().encode(stream2.toByteArray),
        //          StandardCharsets.UTF_8
        //        )
        //        ps.println(retv2)
      }
//      Thread.sleep(100)
      self ! receiveMess
  }
}


object Consumer extends App{
  val host = "localhost"
  val port = 4444
  val sock = new Socket(host, port)
  val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
  val ps = new PrintStream(sock.getOutputStream)
  var sendNow = new AtomicBoolean(true)
  val chosenTopic = scala.util.Random.between(0,2)

  val clientType = "consumer"
  val valueType = List[String]("troopers", "Yoda", "Mandalorians")

  ps.println(clientType)
  ps.println(valueType(chosenTopic))

  val consumerSystem = ActorSystem("derConsumerSystem")

  val consumerMessageReceiver = consumerSystem.actorOf(Props(classOf[ConsumerMessagesReceive], is, ps), "consumerMessagesReceive")

  consumerMessageReceiver ! receiveMess
}