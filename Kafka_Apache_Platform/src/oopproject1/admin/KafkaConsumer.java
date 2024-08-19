package oopproject1.admin;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
*
* @author pmantouvalos,dzoidakis
*/

public class KafkaConsumer extends KafkaClient {

//Constructor (validation is done automatically by calling the super class's constructor	
public KafkaConsumer(KafkaTopic topic) throws Exception {
	super(topic);
	topic.addConsumer(this);
}
	
@Override	
public <K,V> void  sendMessage(K key,V value)throws IllegalStateException {
	
	throw new IllegalStateException("Consumers cannot send messages.");


}
@Override
public void receiveMessage(){
	
	int consumersIndex=0;
	int partitionIndexRangeFirst;//start position of partition at the range set
	int partitionIndexRangeLast;//end position of partition at the range set
	
    for(int i = 0; i <this.getTopic().getConsumers().size();i++)
    {
	  if( this.getTopic().getConsumers().get(i).equals(this)){
		  consumersIndex=i;
	  }
    }
    
    List<Integer> listWithPartitionRange = KafkaTopic.distributePartitions(this.getTopic().getNumPartitions(),this.getTopic().getConsumers().size()).get(consumersIndex);
  
	
    
    partitionIndexRangeFirst = listWithPartitionRange.get(0);
    partitionIndexRangeLast = listWithPartitionRange.get(1);
    
        
    if ( partitionIndexRangeFirst == -1 && partitionIndexRangeLast == -1){
    	//"This consumer isn't assigned any partitions
    	
    }else {
    	if ( partitionIndexRangeFirst == partitionIndexRangeLast){
    	
    		while(!(this.getTopic().getPartitions().get(partitionIndexRangeFirst).getMessagesQueue().isEmpty())) {
    			getTopic().getPartitions().get(partitionIndexRangeFirst).getMessagesQueue().peek().printMessage();
    			Message message = getTopic().getPartitions().get(partitionIndexRangeFirst).getMessagesQueue().poll();
    			getTopic().getMessageStorager().push(message);                                                                          
    			getTopic().getPartitions().get(partitionIndexRangeFirst).setOffset(getTopic().getPartitions().get(partitionIndexRangeFirst).getOffset()+1L);
         
    			for(KafkaReplica kr :this.getTopic().getPartitions().get(partitionIndexRangeFirst).getReplicas()) 
    			{
			 
    				kr.setOffset(this.getTopic().getPartitions().get(partitionIndexRangeFirst).getOffset());
		 
    			}
                                                                                                               
    		}
 	 
       	}
    	else{   
    
    		for (int i = partitionIndexRangeFirst; i <= partitionIndexRangeLast; i++) {
            
    			while(!(this.getTopic().getPartitions().get(i).getMessagesQueue().isEmpty())) {
    				getTopic().getPartitions().get(i).getMessagesQueue().peek().printMessage();
    				Message message = getTopic().getPartitions().get(i).getMessagesQueue().poll();
    				getTopic().getMessageStorager().push(message);                                                                    
    				getTopic().getPartitions().get(i).setOffset(getTopic().getPartitions().get(i).getOffset()+1L);
                
    					for(KafkaReplica kr :this.getTopic().getPartitions().get(i).getReplicas()) 
    					{
   				 
    						kr.setOffset(this.getTopic().getPartitions().get(i).getOffset());
   			 
    					}
                                                                                                                   
    			}
    		}
    	}
    	
    }
}





















}