package oopproject1.admin;

import java.util.PriorityQueue;

/**
*
* @author pmantouvalos,dzoidakis
*/

public class KafkaReplica {
private KafkaBroker broker;
private KafkaPartition partition;
private PriorityQueue<Message> messages;

private static final long DEFAULT_OFFSET=0;
private long offset;

//Constructor with validation
protected KafkaReplica(KafkaBroker broker, KafkaPartition partition) throws  NullPointerException{
	
	if(KafkaTopic.checkNullObject(broker)) {
		throw new  NullPointerException("Kafka broker cannot be null");
	}
	
	if(KafkaTopic.checkNullObject(partition)) {
		throw new  NullPointerException("Kafka partition cannot be null");
	}
	
	this.broker = broker;
	this.partition = partition;
	this. messages=new PriorityQueue<>();
	
	this.offset=DEFAULT_OFFSET;
}


//Getters and Setters with validation (All the expressly mentioned unnecessary setters/getters were removed)
protected KafkaBroker getBroker() {
	return broker;
}

protected KafkaPartition getPartition() {
	return this.partition;
}

protected PriorityQueue<Message> getMessages() {
	return messages;
}

protected void setOffset(long offset) throws IllegalArgumentException {
	
	if(offset > 0 && offset <= Long.MAX_VALUE) {
		this.offset = offset;
	}
     else {
        throw new IllegalArgumentException("Invalid offset.");
    }
}

protected long getOffset() {
	return this.offset;
}


protected static long getDefaultOffset() {
	return DEFAULT_OFFSET;
}


protected void setBroker(KafkaBroker broker) throws NullPointerException{
	
	
	if(KafkaTopic.checkNullObject(broker)) {
		throw new  NullPointerException("Kafka broker cannot be null");
	}
	
	this.broker = broker;

}


protected void setPartition(KafkaPartition partition) throws  NullPointerException{
	
	if(KafkaTopic.checkNullObject(partition)) {
		throw new  NullPointerException("Kafka partition cannot be null");
	}
		
	this.partition = partition;
}


protected void setMessages(PriorityQueue<Message> messages) {
	this.messages = messages;
}














































}
