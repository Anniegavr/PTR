package lab3
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}

import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader, ObjectInputStream, ObjectOutputStream, PrintStream}
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.{Base64, Properties, UUID}

//TODO create objects for consumer's commands
case object listenForConsumerMess
case object ListenForConfirm
case object sendMess

//TODO create objects for producer's commands
case object listenForMess

//TODO create objects for broker's commands
case object listenForConn

//TODO create class to receive confirm from consumer
class ConfirmationReceiver(is: BufferedReader, msgManager: ActorRef) extends Actor{ //from consumer
  val messages = util.ArrayList[Confirm]()

  def receive = {
    case listenForConsumerMess =>
      if (is.ready) { //if there is a message coming
        val input = is.readLine
        val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case msg: Confirm =>
            println("Message received: id:" + msg.id)
            messages.add(msg)
            msgManager ! msg  //this is the manager
          case _ => throw new Exception("I didn't subscribe for dis, scammer")
        }
        ois.close()
      }
      Thread.sleep(10)
      self ! listenForConsumerMess
  }
}
////TODO fix the duplicate
//class ConfirmationReceiver() extends Actor{
//  val messagesToManage = ConcurrentLinkedDeque[ConsumerToAck]()
//  def receive = {
//    case cons2ack : ConsumerToAck =>
//      println("Adding:" + cons2ack.toString)
//      messagesToManage.add(cons2ack)
//      println("messages in manager: " + messagesToManage.toString)
//      //      messagesToManage.forEach(x =>{
//      self ! ListenForConfirm
//    //      })
//    case ListenForConfirm =>
//      Thread.sleep(100)
//      messagesToManage.forEach(some=>{
//        println("checking confirm from:" + some)
//        if(some.consumersCommunication.is.ready()) {
//          val input = some.consumersCommunication.is.readLine
//
//          val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))
//
//          val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
//          ois.readObject match {
//            case confirm: Confirm =>
//              println("Confirmation received:" + confirm.toString)
//              messagesToManage.remove(some)
//            case _ => throw new Exception("Dis is not a message from client")
//          }
//          ois.close()
//        }
//      })
//  }
//}

/**
 * Actor that sends messages to its dedicated consumer (1 MsgSender per each consumer)
 * @param os - outpur stream - the stream to which the sender publishes messages
 */
class MsgSender(os: PrintStream) extends Actor{
  def receive = {
    case SendMess =>
      val orderNo = 0
      val msg = new Message((orderNo+1).toString+"&"+UUID.randomUUID().toString, 1, "troopers", 77)
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

/**
 * Dedicated queue to receive messages from its producer
 * @param is - a buffered reader through which the broker receives and decodes messages
 * @param messageManager
 */
class MessageReceivingQueue(is: BufferedReader, messageManager: ActorRef) extends Actor{
  val messages = util.ArrayList[Message]()

  def receive = {
    case listenForMess =>
      if (is.ready) { //if there is a message coming
        val input = is.readLine
        val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case msg: Message =>
            println("Message received: id:" + msg.id + ", topic:" + msg.topic + ", value:" + msg.value)
            messages.add(msg)
            messageManager ! msg  //this is the manager
          case _ => throw new Exception("Dis is not a message from client, scammer")
        }
        ois.close()
      }
      Thread.sleep(10)
      self ! listenForMess
  }
}

/**
 * Accepts new clients to the broker and takes care of the connection. 1 instance for all producers and consumers.
 * It creates the MessagesReceiving, MsgSender & the ConfirmationReceiver actors.
 * @param ss - socket connection
 * @param messageManager - reference to the actor that manages messages to be sent to consumer
 * @param messagesConfirmer - reference to the actor on the consumer's side that sends confirmation back to the broker
 */
class ConnectionActor(ss: ServerSocket, messageManager : ActorRef, messagesConfirmer : ActorRef) extends Actor {
  val allProducers = util.ArrayList[String]()
  val allConsumers = util.ArrayList[String]()
  def receive = {
    case listenForConn =>
      val sock = ss.accept()
      val subscribedTopic:Map[UUID,String] = Map()
      val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
      val os = new PrintStream(sock.getOutputStream())
      val uuid = UUID.randomUUID()
      println("Client connected: " + uuid.toString.substring(0, 4))
      val clientType = is.readLine //Reading the type of client
      if (clientType.equals("producer")) {
        allProducers.add(uuid.toString)
        val messageReceivingQueue = context.actorOf(Props(classOf[MessageReceivingQueue], is, messageManager), "messageReceivingQueueActor"+uuid)
        messageReceivingQueue ! listenForMess
        println("Producer connected")
      } else if (clientType.equals("consumer")) {
        allConsumers.add(uuid.toString)
        val topics = is.readLine //should the consumer really subscribe to more than one topic? str.include?ini
        println(topics)
        subscribedTopic.+(uuid -> topics)
        println("Consumer connected for topic "+topics )
        messageManager ! ConsumersCommunication(os, is, topics) //comment it
        //TODO 1. create an actor to send messages from manager to consumer, send him the os
        val messageSender = context.actorOf(Props(classOf[MsgSender], is, messagesConfirmer), "messageSenderForConsumerQueueActor"+uuid)
        println("Dedicated message sender to consumer actor init-ed")
        messageSender ! sendMess
        //TODO 2.create actor for receiving messges from consumer, send it the is
        val confirmationReceiver = context.actorOf(Props(classOf[ConfirmationReceiver], is, messageManager), "messageSender"+uuid)
        confirmationReceiver ! listenForConsumerMess
        println("Consumer confirmation rcv queue actor init-ed")
//        val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
//        val oos = new ObjectOutputStream(stream)
//        oos.writeObject(message)
//        oos.close()
//        val retv = new String(
//          Base64.getEncoder().encode(stream.toByteArray),
//          StandardCharsets.UTF_8
//        )
//        if(consumer.topic.equals(message.topic)){
//          println("sending to consumer")
//          consumer.os.println(retv)
//        }

          //to receive - another actor
//        if (is.ready) { //if there is a message coming
//          val input = is.readLine
//          val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))
//
//          val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
//          ois.readObject match {
//            case msg: Message =>
//              println("Message received: id:" + msg.id + ", topic:" + msg.topic + ", value:" + msg.value)
//              messages.add(msg)
//              actorForAllMess ! msg  //this is the manager
//            case _ => throw new Exception("Dis is not a message from client, scammer")
//          }
//          ois.close()
      }
      self ! listenForConn
  }
}

/**
 * Actor that passes the producer's messages (from each producer's MessageReceiving queue) to the correct consumer's queue('s).
 * Also, it tells the messages confirmer which consumer has to acknowledge specific messages. 1 instance for all producers and consumers.
 * @param messageConfirmer - reference to the actor that manages the list of consumers and the messages they have to confirm
 */
class MessageManager(messageConfirmer : ActorRef) extends Actor{
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
        messageConfirmer ! ConsumerToAck(consumer, message)
      }
      )
    case os : ConsumersCommunication =>
      consumersList.add(os)
  }
}


object MessageBroker  extends App{
  val ss = new ServerSocket(4444)
  val brokerSystem = ActorSystem("derDealer")
  val messageManager = brokerSystem.actorOf(Props(classOf[ConfirmationReceiver]), "messageManagerName")
  val actorForAllMess = brokerSystem.actorOf(Props(classOf[MessageManager], messageManager), "actorForAllMessName")
  //TODO create the class messagesConfirmer
  val messagesConfirmer = brokerSystem.actorOf(Props(classOf[]))

  val actorForConnection = brokerSystem.actorOf(Props(classOf[ConnectionActor], ss, actorForAllMess, messagesConfirmer), "actorForConnection")
  actorForConnection ! listenForConn
}
