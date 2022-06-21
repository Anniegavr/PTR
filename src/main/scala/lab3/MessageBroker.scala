package lab3
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}

import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader, ObjectInputStream, ObjectOutputStream, PrintStream}
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.{Base64, Comparator, Properties, UUID}
import java.io.*
import scala.io.Source
import scala.util.Try
//objects for consumer's commands
case object listenForConsumerMess
case object listenForConfirm
case object sendMessToConsumer

//objects for producer's commands
case object listenForMess

//objects for broker's commands
case object listenForConn
case object getToWork
case object sortThisList
case object sendBackSortedList
/**
 * Class responsible to receive confirmation from the consumer
 * @param is - the input stream through which the consumer and the ConfirmationReceiver communicate
 * @param messageSender - the consumer's dedicated sender's reference
 * @param connectionActor - reference to the actor responsible for accepting connections and starting corresponding actors
 */
class ConfirmationReceiver(is: BufferedReader, messageSender: ActorRef, connectionActor: ActorRef) extends Actor{
  def receive: Receive = {
    case listenForConsumerMess =>
      if (is.ready) { //if there is a message coming
        val input = is.readLine
        val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case msg: Confirm =>
            println("Confirmation received: id:" + msg.id + "for topic "+ msg.topic)
            messageSender ! msg
            connectionActor ! ConfirmedMess(msg.id, msg.topic)
          case _ => throw new Exception("I didn't subscribe for dis, scammer")
        }
        ois.close()
      }
      Thread.sleep(10000)
      self ! listenForConsumerMess
  }
}

/**
 * Actor that sends messages to its dedicated consumer (1 MsgSender per each consumer)
 * @param os - output stream - the stream to which the sender publishes messages
 */
class MsgSender(os: PrintStream) extends Actor{
  val messagesToSend: util.ArrayList[Message] = util.ArrayList[Message]()

  override def postStop(): Unit = {
    println("Sender stopped :P ")
  }
  override def preStart(): Unit = {
    println("Sender: I am ready")
  }
  def receive: Receive = {
    case message : Message =>
      println("Sender: New message with id "+message.id)

      messagesToSend.add(message)
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(messagesToSend.get(0))
      oos.close()
      val retv = new String(
        Base64.getEncoder.encode(stream.toByteArray),
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
 * @param messageManager - reference to this context's message manager
 * @param connectionActor - reference to the actor responsible for accepting connections and starting corresponding actors
 */
class MessageReceiving(is: BufferedReader, messageManager: ActorRef, connectionActor: ActorRef) extends Actor{
  val messages: util.ArrayList[Message] = util.ArrayList[Message]()

  def receive: Receive = {
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
      Thread.sleep(1000)
      self ! listenForMess
    case quit =>
      is.close()
      connectionActor ! ProducerQuit(self)

  }
}

/**
 * Accepts new clients to the broker and takes care of the connection. 1 instance for all producers and consumers.
 * It creates the MessagesReceiving, MsgSender & the ConfirmationReceiver actors.
 * @param ss - socket connection
 * @param messageManager - reference to the actor that manages messages to be sent to consumer
 */
class ConnectionActor(ss: ServerSocket, messageManager : ActorRef) extends Actor {
  val allProducers: util.ArrayList[String] = util.ArrayList[String]()
  val allConsumers: util.ArrayList[String] = util.ArrayList[String]()
  var allProducersFiles: util.ArrayList[File] = util.ArrayList[File]()
  def receive: Receive = {
//    case quit =>
//      ss.close()
    case listenForConn =>
      val sock = ss.accept()
      val subscribedTopic:Map[String,String] = Map()
      val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
      val os = new PrintStream(sock.getOutputStream)
      val uuid = UUID.randomUUID()
      println("Client connected: " + uuid.toString.substring(0, 4))
      val clientType = is.readLine //Reading the type of client
      if (clientType.equals("producer")) {
        allProducers.add(uuid.toString)
        val fileObject = new File("producer/producerStream_"+is+".txt" )
        allProducersFiles.add(fileObject)
        //is for receving msges from producer & msgManager to send the received mesg
        val messageReceiving = context.actorOf(Props(classOf[MessageReceiving], is, messageManager, self), "messageReceivingActor"+uuid)
        messageReceiving ! listenForMess
        println("Producer connected")
      } else if (clientType.equals("consumer")) {
        allConsumers.add(uuid.toString)
        val topics = is.readLine
        println(topics)
//        subscribedTopic.+(uuid -> topics)
        println("Consumer connected for topic "+topics )
        //an actor to send messages from manager to consumer, send him the os
        val msgSender = context.actorOf(Props(classOf[MsgSender], os), "messageSenderForConsumerQueueActor"+uuid)
        println("Dedicated message sender to consumer actor init-ed")
        messageManager ! ConsumersCommunication(msgSender, topics)
        
        //an actor for receiving messges from consumer, send it the is
        val confirmationReceiver = context.actorOf(Props(classOf[ConfirmationReceiver], is, msgSender, self), "confirmationReceiver"+uuid)
        confirmationReceiver ! listenForConsumerMess
        println("Consumer confirmation rcv queue actor init-ed")
      }
      self ! listenForConn
    case producerQuit: ProducerQuit =>
      allProducers.remove(producerQuit.producer)
    case confirmedMess : ConfirmedMess =>
      messageManager ! ConfirmedMess(confirmedMess.messageId, confirmedMess.messageTopic)
  }
}

/**
 * Actor that uses selection sort to sort the provided list in accordance to the messages' priorities (ascending)
 * Priority 1 = highest
 * @param listToSort - the provided ArrayList[Message]
 * @param messageManager - reference to the MessageManager that has created and communicated with this ListSorter actor
 */
class ListSorter(listToSort : util.ArrayList[Message], messageManager : ActorRef) extends Actor{
  var someList: util.ArrayList[Message] = listToSort
  def sortL(): Unit = {
    val n = someList.size()
//    someList.sort(_.priority.<(:Message))
    someList.forEach( mess => {
      var min_idx = someList.indexOf(mess)
      val i = min_idx
      val j = i+1
      val temp: Message = someList.get(min_idx)
      for (j <- i + 1 until n) {
        if (someList.get(j).priority < someList.get(min_idx).priority) {
          min_idx = j
        }
        // Swap the found minimum element with the first
        // element

        someList.set(min_idx, someList.get(i))
        someList.set(i, temp)
      }
    })
  }
  def receive: Receive = {
    case sortThisList =>
      println("starting to sort")
      if (someList.size() > 0){
        sortL()
        messageManager ! SortedList(someList, someList.get(0).topic)
        println("sorted list size: "+someList.size())

      } else {
        println("Finshed to sort empty list, kiss")
        messageManager ! SortedList(someList, someList.get(0).topic)
      }

//      self ! sendBackSortedList
//    case sendBackSortedList =>

  }
}

/**
 * Actor that passes the producer's messages (from each producer's MessageReceiving queue) to the correct consumer's queue('s).
 * Also, it tells the messages confirmer which consumer has to acknowledge specific messages. 1 instance for all producers and consumers.
 */
class MessageManager() extends Actor{
  val consumersList: util.ArrayList[ConsumersCommunication] = util.ArrayList[ConsumersCommunication]()
  val allMessagesLst: util.ArrayList[Message] = util.ArrayList[Message]()
  val topicTroopers: util.ArrayList[Message] = util.ArrayList[Message]()
  val topicYoda: util.ArrayList[Message] = util.ArrayList[Message]()
  val topicMandalorian: util.ArrayList[Message] = util.ArrayList[Message]()
  val yodaSorterActor: ActorRef = context.actorOf(Props(classOf[ListSorter], topicYoda, self))
  val troopersSorterActor: ActorRef = context.actorOf(Props(classOf[ListSorter], topicTroopers, self))
  val mandalorianSorterActor: ActorRef = context.actorOf(Props(classOf[ListSorter], topicMandalorian, self))
  var unrecorderMessages = 0
  var messageNeeds2bSent = true
  val file = new File("allMessages.txt")

  override def preStart(): Unit = {
    Try {
      val sendTheseMessages = util.ArrayList[Message]()
      val history = io.Source.fromFile("allMessages.txt")
      val lines = history.getLines()
      val separatedValuesInMess = lines.next().split(" ")
      val messId = separatedValuesInMess(0)
      val messPriority = separatedValuesInMess(1)
      val messTopic = separatedValuesInMess(2)
      val messValue = separatedValuesInMess(3)
      val oneMess = new Message(messId,Integer.parseInt(messPriority),messTopic, Integer.parseInt(messValue))
      self ! oneMess
      println("Manager: prestart finished")
      history.close()
      }
    }

  def receive: Receive = {
    case connection: ConsumersCommunication =>
      consumersList.add(connection)
    case message: Message =>
      println("adding new message in common list for this topic, id = " + message.id)
      if (message.topic == "Yoda") {
        topicYoda.add(message)
        if (topicYoda.size > 2){
          yodaSorterActor ! sortThisList
        }
      } else if (message.topic == "troopers") {
        topicTroopers.add(message)
        if (topicTroopers.size > 2){
          troopersSorterActor ! sortThisList
        }

      } else if (message.topic == "Mandalorians") {
        topicMandalorian.add(message)
        if (topicTroopers.size > 2){
          mandalorianSorterActor ! sortThisList
        }
      }
      unrecorderMessages+=1
      if (unrecorderMessages == 20){
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(topicYoda.toString)
        bw.write(topicTroopers.toString)
        bw.write(topicMandalorian.toString)

        bw.close()
      }
    case sender: ConsumersCommunication =>
      consumersList.add(sender)
    case sortedList: SortedList =>
      println("Manager: got the sorted list back for " + sortedList.topic + "\n list is: "+sortedList.sortedList)
      if (sortedList.topic == "Yoda") {
        topicYoda.clear()
        topicYoda.addAll(sortedList.sortedList)
      } else if (sortedList.topic == "troopers") {
        topicTroopers.clear()
        topicTroopers.addAll(sortedList.sortedList)
      } else if (sortedList.topic == "Mandalorians") {
        topicMandalorian.clear()
        topicMandalorian.addAll(sortedList.sortedList)
      }
      messageNeeds2bSent = true
      self ! getToWork
    case getToWork =>
      println("GOOD")

      topicYoda.forEach(yodaMessage => {
        println("manager: msg need sent "+ messageNeeds2bSent)
        if (messageNeeds2bSent) {
          println("Manager: checking Yoda message")
          consumersList.forEach(consumer => {
            println("Manager: for each consumer ")
            if (consumer.topic == "Yoda") {
              consumer.sender ! yodaMessage
              messageNeeds2bSent = false
            }
          })
        }
      })
      topicTroopers.forEach( troopMessage => {
        if (messageNeeds2bSent) {
          consumersList.forEach(consumer => {
            if (consumer.topic == "troopers") {
              consumer.sender ! troopMessage
              messageNeeds2bSent = false
            }
          })
        }
      })
      topicMandalorian.forEach( mandalorianMessage => {
        if (messageNeeds2bSent) {
          consumersList.forEach(consumer => {
            if (consumer.topic == "Mandalorians") {
              consumer.sender ! mandalorianMessage
              messageNeeds2bSent = false
            }
          })
        }
      })
//        println("Manager: ich bin gut boy")
  }
}

object MessageBroker  extends App{
  val ss = new ServerSocket(4444)
  val brokerSystem = ActorSystem("derDealer")
  val messageManager = brokerSystem.actorOf(Props(classOf[MessageManager]), "actorForAllMessName")
  val actorForConnection = brokerSystem.actorOf(Props(classOf[ConnectionActor], ss, messageManager), "actorForConnection")
  actorForConnection ! listenForConn
  while(true){
    Thread.sleep(1000)
  }
}
