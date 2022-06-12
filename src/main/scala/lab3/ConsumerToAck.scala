package lab3

/**
 * Class representing info about the consumer that has to confirm the message. Might be removed.
 * @param consumersCommunication - the stream throught which the broker is connected to the consumer
 * @param message
 */
case class ConsumerToAck(consumersCommunication: ConsumersCommunication, message: Message)
