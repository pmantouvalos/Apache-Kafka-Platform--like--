package oopproject1.admin;

import java.util.ArrayList;
import java.util.PriorityQueue;

/**
*
* @author pmantouvalos,dzoidakis
*/

public class KafkaPartition {

private long offset;
private ArrayList<KafkaReplica> replicas;
private static final int  DEFAULT_NUM_REPLICAS=3;
private static final long DEFAULT_OFFSET=0;
private int replicaCount;
private PriorityQueue<Message> messagesQueue;
private int numReplicas;

//------------------Constructor with validation----------------------
protected KafkaPartition(int numReplicas) throws IllegalArgumentException {
	
	if(!(KafkaTopic.checkValidPositiveInteger(numReplicas))) {
		 throw new IllegalArgumentException("Number of replicas must be greater than zero");
	}
	
	this.numReplicas=numReplicas;
	
	this.replicas= new ArrayList<>();
	
	this.offset=DEFAULT_OFFSET; 
    this.replicaCount=0;
    
    messagesQueue=new PriorityQueue<>();
    

}

//Overloaded constructor with default numReplicas




private KafkaPartition() {
	this(DEFAULT_NUM_REPLICAS);
}


//Getters and Setters with validation (All the expressly mentioned unnecessary setters/getters were removed)

protected PriorityQueue< Message> getMessagesQueue(){
	return this.messagesQueue;
}


//offset can change
protected void setOffset(long offset) throws IllegalArgumentException {
		
		if(offset > 0 && offset <= Long.MAX_VALUE) {
			this.offset = offset;
		}
	     else {
	        throw new IllegalArgumentException("Invalid offset.");
	    }
	}

protected ArrayList<KafkaReplica> getReplicas() {
	return this.replicas;
}

public void setReplicaCount(int replicaCount) throws IllegalArgumentException{

	if (replicaCount<0) {
	        throw new IllegalArgumentException("Replica count must be a valid positive integer.");
	    }
	    
	    this.replicaCount = replicaCount;
}

protected void setReplicas(ArrayList<KafkaReplica> replicas) throws IllegalArgumentException{
	
	if (KafkaTopic.checkNullObject(replicas)) {
        throw new IllegalArgumentException("Replicas array cannot be null.");
    }
    
    for (KafkaReplica replica : replicas) {
        if (KafkaTopic.checkNullObject(replica)) {
            throw new IllegalArgumentException("Replicas array cannot contain null elements.");
        }
    }
    
    
    this.replicas = replicas;
    this.replicaCount=replicas.size();
}

protected int getReplicasSize() {
	return replicas.size();
}
protected long getOffset() {
	return this.offset;
}


protected static int getDefaultNumReplicas() {
	return DEFAULT_NUM_REPLICAS;
}

protected static long getDefaultOffset() {
	return DEFAULT_OFFSET;
}

protected int getReplicaCount() {
	return replicaCount;
}

protected void setMessagesQueue(PriorityQueue<Message> messagesQueue) {
	this.messagesQueue = messagesQueue;
}


//------------------------------methods-------------------------------



protected void addReplica(KafkaReplica replica) throws IllegalStateException{

	if (replicas.size() <this.numReplicas) /*We have to retain the bounds in our ArrayLists as they were before in the arrays*/
	       {                         
		   replicas.add(replica);
		   
	} else {
		        throw new IllegalStateException("Cannot add more replicas. Partition is full of replicas.");
		      }
		

}

}










