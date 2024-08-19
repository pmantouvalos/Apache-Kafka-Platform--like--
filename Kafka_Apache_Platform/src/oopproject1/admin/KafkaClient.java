package oopproject1.admin;

/**
*
* @author pmantouvalos,dzoidakis
*/

public abstract class KafkaClient {
private KafkaTopic topic;
protected abstract <K,V> void sendMessage(K key, V value)throws Exception;
protected abstract void receiveMessage()throws Exception;


//Constructor with validation
protected KafkaClient(KafkaTopic topic) throws Exception{
	
	if(KafkaTopic.checkNullObject(topic)) {
		throw new NullPointerException("Kafka topic cannot be null");
	}
	
	
	this.topic=topic;
	
}

//Getters and Setters with validation (All the expressly mentioned unnecessary setters/getters were removed)
protected KafkaTopic getTopic() {
	return topic;
}
private void setTopic(KafkaTopic topic) throws NullPointerException{
	
	if(KafkaTopic.checkNullObject(topic)) {
		throw new NullPointerException("Kafka topic cannot be null");
	}
	
	this.topic = topic;
}





































//-----------------methods----------------------

//through this check,we are checking if a client is already sending or receiving messages to/from another topic. If yes, we throw an exception

















}
