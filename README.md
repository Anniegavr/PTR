# PTR
Real Time Programming Course

## Checkpoint 1
The visualization of the project's main concept can be found in the docs folder.
 
## Checkpoint 2
### How does this project work?
The project itself is build usint the Lab3/build.sbt file. It knows what dependencies to work with, as well as their verions.

the src directory (src->main->scala->lab3) contains the actual code:
 - Consummer - the data type behind the actual concept of a message consummer. It contains a class "ConsumerMessagesReceive" that takes/holds information about the input stream ("is") through which the broker sends messages to it, and the print stream ("ps") through which the consummer sends messages.
 - Message - the data type behind the actual concept of a message. It takes/holds information about the id of the message, its priority (yet to be implemmented in the functional), the topic to which it belongs and the value of this message.
 - Confirm.scala - used to send the message confirmation. A message of type "Confirm" is serialized and sent by the consummer when acknowledging the receipt of a message from the broker. As arguments, it holds the id of the message that has to be confirmed.
 - ConsummersCommunication - used to hold the information about the output stream ("os") and the values sent through it - from the broker, to the consummers.
 - ConsummerToAck - used as a type holding information about the stream through which the consummer is connected to the broker and the message this consummer has to receive and acknowledge.
 - MessageBroker - the data type behind the actual concept of a message broker. It contains a class "MessageReceivingQueue" that takes/holds information about the input stream through which it receives messages from the producer and a reference to the manager of all the sent messages that have to be acknowledged by the consummer. So when this queue receives a message, it sends it to the "MessageSendingQueue" which, in turn, sends all the necessarry messages to the consummers. At the same time, this MessageSendingQueue sends the information about the consummer that should have received a message - ConsummerToAck - to the "SentMessagesManager". This manager then waits for a message of type Confirmation to come from the consummer and looks in its queue to see which message has been acknowledged and is ready to be removed from the queue.

![image](https://user-images.githubusercontent.com/56108881/170884410-230f36b1-be34-44ed-ac61-e85eb99c9d4f.png)

More of it, in the docs/Checkpoint1 file. The funtionalities are yet to be enhanced towards a clearer and smoother workflow.

Next steps: finish implementation details and dockerize components.

### Structure of the main applications - Producer, Consummer, Message Broker
In some ways, the three of them are similar. To create them, a class that extends App is needed, for the component to work as a separate application that can be dockerized. They all contain information about the port they work with - 4444, Telnet. They connect through a socket, instaintiate an actor system, as well as input/output streams (for producers and consummers).
![image](https://user-images.githubusercontent.com/56108881/170884564-b4896ed0-e95b-47ec-9178-1661102666fa.png)
![image](https://user-images.githubusercontent.com/56108881/170884596-70a4c558-0ffe-4377-beb6-cc142002cc24.png)
![image](https://user-images.githubusercontent.com/56108881/170884609-54c119a6-b994-4cf9-a633-05aacd88bf9e.png)

Aside from this, the each system has its own actors meant to receive, process and pass messages. To create them, I declared the class using a relevant name (a SentMessagesManager will manage messages being sent) and added "extends Actor" in the declaration. Also, I added this actor to its appropriate system in the application it belongs to: "val messageManager = brokerSystem.actorOf(Props(classOf[SentMessagesManager]), "messageManagerName")".

To send commands, I have mostly created and instantiated case classes, just for commands to be recognized for their type and nothing more. I f a command had to hold and pass more info that its own type, I created separate files for their classes - like the Confirmation class.
