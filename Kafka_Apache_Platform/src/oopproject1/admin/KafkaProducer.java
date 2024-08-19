package oopproject1.admin;
import java.util.Iterator;

/**
*
* @author pmantouvalos,dzoidakis
* 
*/

public class KafkaProducer extends KafkaClient {
	
	private int curPartition = 0; 
	
//Constructor (validation is done automatically by calling the super class's constructor
public KafkaProducer(KafkaTopic topic) throws Exception{
    super(topic);
     topic.addProducer(this);
}

private static int hash(String key, int numPartitions) {
	 int sum = 0;
	 for (int i = 0; i < key.length(); i++)
	 sum += (int) key.charAt(i); // Add ASCII value of each character
	return sum % numPartitions; // Take modulo numPartitions

	}


@Override	
protected <K,V> void sendMessage(K key, V value) throws Exception{
	
	if(this.getTopic().getKeyed()==true) {
		
		if((key!=null && !(String.valueOf(key).trim().isEmpty())) && !(String.valueOf(value).trim().isEmpty())) {
			
			KeyedMessage <K,V> km=new KeyedMessage<>(key,value);
			
			KafkaPartition partitionToAddMessage = this.getTopic().getPartitions().get(KafkaProducer.hash(String.valueOf(key),this.getTopic().getNumPartitions()));
				
			partitionToAddMessage.getMessagesQueue().add(km);
			
			for(KafkaReplica replicaToAddMessage :partitionToAddMessage.getReplicas()) {
				replicaToAddMessage.getMessages().add(km);
			}
					
		}else{throw new IllegalArgumentException("The message's value or key cant be null or empty");}
	}
	else{throw new IllegalArgumentException("You cannot send KeyedMessages to NonKeyed topics.");}
		
}
	

protected <V> void sendMessage(V value) throws Exception{
	
	if(this.getTopic().getKeyed()==false) {
		
		if(value!=null && !(String.valueOf(value).trim().isEmpty())) {
			
			NonKeyedMessage <V> nkm=new NonKeyedMessage<>(value);
			
			  int partitionCount = this.getTopic().getNumPartitions();
              this.getTopic().getPartitions().get(curPartition).getMessagesQueue().add(nkm);
              
              for(KafkaReplica replicaToAddMessage :this.getTopic().getPartitions().get(curPartition).getReplicas()) {
  				replicaToAddMessage.getMessages().add(nkm);
  			  }
              curPartition = (curPartition + 1) % partitionCount;
			
		}else{throw new IllegalArgumentException("The message's value cant be null or empty");}
	}
		else{throw new IllegalArgumentException("You cannot send NonKeyed messages to keyed topic");}
	
}

@Override
protected void receiveMessage() throws IllegalStateException{
	
	throw new IllegalStateException("Producers cannot receive messages.");

}


//Getters and Setters with validation (All the expressly mentioned unnecessary setters/getters were removed)
protected int getCurPartition() {
	return curPartition;
}

protected void setCurPartition(int curPartition) {
	this.curPartition = curPartition;
}

}