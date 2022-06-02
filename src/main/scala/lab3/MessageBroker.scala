package lab3
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}

import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader, ObjectInputStream, ObjectOutputStream, PrintStream}
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.{Base64, Properties, UUID}

case object listenForMess

class MessageReceivingQueue(is: BufferedReader, actorForAllMess: ActorRef) extends Actor{
  val messages = util.ArrayList[Message]()

  def receive = {
    case listenForMess =>
      if (is.ready) {
        val input = is.readLine

        val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case msg: Message =>
            println("Message received: id:" + msg.id + ", topic:" + msg.topic + ", value:" + msg.value)
            messages.add(msg)
            actorForAllMess ! msg
          case _ => throw new Exception("Dis is not a message from client, scammer")
        }
        ois.close()

      }
      Thread.sleep(10)
      self ! listenForMess
  }
}
case object listenForConn

class ActorForConnection(ss: ServerSocket, actorForAllMess : ActorRef) extends Actor {

  def receive = {
    case listenForConn =>
      val sock = ss.accept()
      val allProducers = util.ArrayList[String]()
      val allConsumers = util.ArrayList[String]()
      val subscribedTopic:Map[UUID,String] = Map()
      val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
      val os = new PrintStream(sock.getOutputStream())
      val uuid = UUID.randomUUID()
      println("Client connected: " + uuid.toString.substring(0, 4))
      val clientType = is.readLine //Reading the type of client
      if (clientType.equals("producer")) {
        allProducers.add(uuid.toString)
        val messageReceivingQueue = context.actorOf(Props(classOf[MessageReceivingQueue], is, actorForAllMess), "messageReceivingQueueActor")
        messageReceivingQueue ! listenForMess
        println("Producer connected")
      } else if (clientType.equals("consumer")) {
        allConsumers.add(uuid.toString)
        val topics = is.readLine
        println(topics)
        subscribedTopic.+(uuid -> topics)
        println("Consumer connected for topic "+topics )
        actorForAllMess ! ConsumersCommunication(os, is, topics)
      }
      self ! listenForConn
  }
}

class ReceiveAllMessActor(messManager : ActorRef) extends Actor{
  val allMessages = util.ArrayList[Message]()
  val consumersList = util.ArrayList[ConsumersCommunication]()
  def receive = {
    case  message : Message =>
      println("adding new message in common list, id = " + message.id)
      allMessages.add(message)
      println("All messages:" + allMessages)
      consumersList.forEach(consumer => {
        val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(stream)
        oos.writeObject(message)
        oos.close()
        val retv = new String(
          Base64.getEncoder().encode(stream.toByteArray),
          StandardCharsets.UTF_8
        )
        
        if(consumer.topic.equals(message.topic)){
          println("sending to consumer")
          consumer.os.println(retv)
        }
        println("sending to manager")
        messManager ! ConsumerToAck(consumer, message)
      }
      )
    case os : ConsumersCommunication =>
      consumersList.add(os)
  }
}

case object ListenForConfirm

class SentMessagesManager() extends Actor{
  val messagesToManage = ConcurrentLinkedDeque[ConsumerToAck]()
  def receive = {
    case cons2ack : ConsumerToAck =>
      println("Adding:" + cons2ack.toString)
      messagesToManage.add(cons2ack)
      println("messages in manager: " + messagesToManage.toString)
//      messagesToManage.forEach(x =>{
        self ! ListenForConfirm
//      })
    case ListenForConfirm =>
      Thread.sleep(100)
      messagesToManage.forEach(abc=>{
        println("checking confirm from:" + abc)
        if(abc.consumersCommunication.is.ready()) {
          val input = abc.consumersCommunication.is.readLine

          val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

          val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
          ois.readObject match {
            case confirm: Confirm =>
              println("Confirmation received:" + confirm.toString)
              messagesToManage.remove(abc)
            case _ => throw new Exception("Dis is not a message from client")
          }
          ois.close()
        }
      })
  }
}
object MessageBroker  extends App{
  val ss = new ServerSocket(4444)
  val brokerSystem = ActorSystem("derDealer")
  val messageManager = brokerSystem.actorOf(Props(classOf[SentMessagesManager]), "messageManagerName")
  val actorForAllMess = brokerSystem.actorOf(Props(classOf[ReceiveAllMessActor], messageManager), "actorForAllMessName")


  val actorForConnection = brokerSystem.actorOf(Props(classOf[ActorForConnection], ss, actorForAllMess), "actorForConnection")
  actorForConnection ! listenForConn

}
