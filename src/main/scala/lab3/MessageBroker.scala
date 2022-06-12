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
case object listenForConfirm
case object sendMessToConsumer

//TODO create objects for producer's commands
case object listenForMess

//TODO create objects for broker's commands
case object listenForConn

//TODO create class to receive confirm from consumer
class ConfirmationReceiver(is: BufferedReader, messageSender: ActorRef) extends Actor{ //from consumer
//  val messages = util.ArrayList[Confirm]()

  def receive = {
    case listenForConsumerMess =>
      if (is.ready) { //if there is a message coming
        val input = is.readLine
        val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case msg: Confirm =>
            println("Confirmatyion received: id:" + msg.id)
//            messages.add(msg)
            messageSender ! msg  //this is the manager
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
 * @param os - output stream - the stream to which the sender publishes messages
 */
class MsgSender(os: PrintStream) extends Actor{
  val messagesToSend = util.ArrayList[Message]()

  override def postStop(): Unit = {
    println("Sender stopped :P ")
  }
  def receive = {
//    case sendMessToConsumer =>

//      if (messagesToSend.size() > 0){
//        Thread.sleep(1000)
//        self ! sendMess
//      }
    case message : Message =>
      println("Sender: New message with id "+message.id)

      messagesToSend.add(message)
//      self ! sendMessToConsumer
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      //TODO fix priority instead of hardcoded index - foreach, if
      oos.writeObject(messagesToSend.get(0))
      oos.close()
      val retv = new String(
        Base64.getEncoder().encode(stream.toByteArray),
        StandardCharsets.UTF_8
      )
      os.println(retv)
    case confirm : Confirm =>
      println("Sender received confirmation for "+confirm.id)
      println("Current queue: ")
      var idOfMsgToDelete = 0
      var incrementId = true
      messagesToSend.forEach( message => {
        println(message.id)
        if (confirm.id == message.id) {
          incrementId = false
        }
        if (incrementId){
          idOfMsgToDelete+=1
        }
      })
      println("Sender: removing message "+messagesToSend.get(idOfMsgToDelete).id)
      messagesToSend.remove(idOfMsgToDelete)

      println("Sender: msg 2 send size: "+messagesToSend.size().toString)

  }
}

/**
 * Dedicated queue to receive messages from its producer
 * @param is - a buffered reader through which the broker receives and decodes messages
 * @param messageManager
 */
class MessageReceiving(is: BufferedReader, messageManager: ActorRef) extends Actor{
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
            messageManager ! msg
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
class ConnectionActor(ss: ServerSocket, messageManager : ActorRef) extends Actor {
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
        //is for receving msges from producer & msgManager to send the received mesg
        val messageReceiving = context.actorOf(Props(classOf[MessageReceiving], is, messageManager), "messageReceivingActor"+uuid)
        messageReceiving ! listenForMess
        println("Producer connected")
      } else if (clientType.equals("consumer")) {
        allConsumers.add(uuid.toString)
        val topics = is.readLine //should the consumer really subscribe to more than one topic? str.include?ini
        println(topics)
        subscribedTopic.+(uuid -> topics)
        println("Consumer connected for topic "+topics )

        //TODO 1. create an actor to send messages from manager to consumer, send him the os
        val msgSender = context.actorOf(Props(classOf[MsgSender], os), "messageSenderForConsumerQueueActor"+uuid)
        println("Dedicated message sender to consumer actor init-ed")
        messageManager ! ConsumersCommunication(msgSender, topics) //comment it
//        msgSender ! sendMess
        //TODO 2.create actor for receiving messges from consumer, send it the is
        val confirmationReceiver = context.actorOf(Props(classOf[ConfirmationReceiver], is, msgSender), "confirmationReceiver"+uuid)
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
 * @param messagesConfirmer - reference to the actor that manages the list of consumers and the messages they have to confirm
 */

class MessageManager() extends Actor{
  val consumersList = util.ArrayList[ConsumersCommunication]()
  def receive = {
    case  message : Message =>
      println("adding new message in common list, id = " + message.id)
      consumersList.forEach(consumer => {
        if (consumer.topic == message.topic) {
          println("Manager sending to sender")
          consumer.sender ! message
        }
//        messagesConfirmer ! ConsumerToAck(consumer, message)
      })
    case sender: ConsumersCommunication =>
      consumersList.add(sender)

  }
}


object MessageBroker  extends App{
  val ss = new ServerSocket(4444)
  val brokerSystem = ActorSystem("derDealer")
  val messageManager = brokerSystem.actorOf(Props(classOf[MessageManager]), "actorForAllMessName")

  val actorForConnection = brokerSystem.actorOf(Props(classOf[ConnectionActor], ss, messageManager), "actorForConnection")
  actorForConnection ! listenForConn
}
