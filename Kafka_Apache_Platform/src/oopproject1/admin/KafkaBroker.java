package oopproject1.admin;

import java.util.ArrayList;
import java.util.Iterator;

/**
*
* @author pmantouvalos,dzoidakis
*/

public class KafkaBroker {

private String host;
private int port;
private int brokerId;
private static int myBrokerId=1;
private  ArrayList<KafkaTopic> topics;
private int topicCount;
private int maxTopics;

//Constructor with validation

public KafkaBroker(String host, int port, int maxTopics) throws IllegalArgumentException{

	if(!isValidHost(host)){
		throw new IllegalArgumentException("Wrong format of broker's adress host");
	}

	if(!isValidPort(port)) {
		throw new IllegalArgumentException("Port must be an integer between 0 and 65535");
	}

	if(!KafkaTopic.checkValidPositiveInteger(maxTopics)) {
		throw new IllegalArgumentException("Maximum number of topics must be a valid positive integer");
	}

	this.host=host;
	this.port=port;
	this.maxTopics=maxTopics;
	this.topics=new ArrayList<KafkaTopic>(maxTopics);
	this.topicCount=0;
	this.brokerId= myBrokerId++;
}


//Getters and Setters with validation (All the expressly mentioned unnecessary setters/getters were removed)

protected void setTopicCount(int topicCount) throws IllegalArgumentException  {

	if(!KafkaTopic.checkValidPositiveInteger(topicCount)) {
		throw new IllegalArgumentException("Topic count must be a valid positive integer");
	}

	this.topicCount=topicCount;
}
	

protected void setMaxTopics(int maxTopics) throws IllegalArgumentException  {
	if(!KafkaTopic.checkValidPositiveInteger(maxTopics)) {
		throw new IllegalArgumentException("maxTopics must be a valid positive integer");
	}

	this.maxTopics=maxTopics;
}


protected void setTopics(ArrayList<KafkaTopic> topics) throws IllegalArgumentException {
	
	if(KafkaTopic.checkNullObject(topics) || topics.size()>maxTopics) {
	
		throw new IllegalArgumentException("ArrayList topics cannot be null and in must contains less or equal number of topics than maxTopics");
	}
	this.topics = topics;
}



protected void setPort(int port) throws IllegalArgumentException {
	if(!isValidPort(port)) {
		throw new IllegalArgumentException("Port must be a positive integer smaller than 65535");
	}
	else {
	 this.port = port;
	}
}

protected void setHost(String host) throws IllegalArgumentException {
	if(!isValidHost(host)) {
		throw new IllegalArgumentException("Wrong format of broker's adress host");
	}
	else {
	 this.host = host;
	}
}


protected int getTopicCount() {
	return topicCount;
}


public int getMaxTopics() {
	return maxTopics;
}


public String getHost() {
	return host;
}

public int getPort() {
	return port;
}


public int getBrokerId() {
	return brokerId;
}


public ArrayList<KafkaTopic> getTopics() {

	 return topics;
	 
}

protected int getTopicsSize() {
	return topics.size();
}


protected static int getMyBrokerId() {
	return myBrokerId;
}

//-----------------------methods-----------------------


//implementation according to "Input Reading and Processing" slides set
protected static boolean isValidHost(String host) {

if(!KafkaTopic.checkString(host)) {
	return false;
}

String [] tokens= host.split("\\.");

if(tokens.length != 4) {
	return false;
}

for(String token:tokens) {
   try {
	int value= Integer.parseInt(token);
    if(value<0 || value>255) {
	return false;}

   }
catch (NumberFormatException e) {
	return false;
}

}

return true;

}

protected static boolean isValidPort(int port) {
	if((KafkaTopic.checkValidPositiveInteger(port) || port==0) && port<=65535) {
		return true;
	}

return false;

}


//through this function we simply represent the addition of a topic to a broker
/*further checks about this procedure, like if the name of the topic that we try to add is already
 * added to another broker, or if we try to add a topic to a broker that has more topics than some other the time
 * we do the addition, or if two brokers have the same topicCount, in which are we going to add the new topic?*/
 /*we could check if the added topic is null but the way it is initialized from its constructor,makes it impossible
  * to be null  */

protected void addTopic(KafkaTopic topic) throws Exception{	  
			
	if (topics.size() < this.maxTopics) {
	    topics.add(topic);
	    this.topicCount++;
	} else {
	        throw new IllegalStateException("Cannot add more topics. Broker is full of topics.");
	      }
	}

//method to remove a topic from a broker by its name, deleting all partitions and replicas that belong to it
protected void removeTopic(String topicName) throws IllegalArgumentException {
    if (KafkaTopic.checkString(topicName)) {
        boolean topicFound = false;
        Iterator<KafkaTopic> iterator = topics.iterator();

        while (iterator.hasNext()) {
            KafkaTopic topic = iterator.next();
            if (topic.getName().equals(topicName)) {
                iterator.remove();
                this.topicCount--;
                topicFound = true;
                System.out.println("Topic " + topicName + " deleted successfully.");
                break;
            }
        }

        if (!topicFound) {
            throw new IllegalArgumentException("Topic " + topicName + " not found.");
        }
    } else {
        throw new IllegalArgumentException("Topic's name cannot be null or empty.");
    }
}


//topics array will not include null topic objects, so the check is more code in our program without reason altering our DRY approach
protected void listAllTopics() {
	
	for(int i=0;i<topics.size(); i++) {
		if(topics.get(i)!=null) {
		System.out.println("Topic "+(i+1)+": " + topics.get(i).getName());
		}
	}
}

protected void listAllTopics(boolean includeDetails) {
    if (!includeDetails) {
        listAllTopics();
        return;
    } 
    
    if (topics.size() == 0) {
        System.out.println("No topics found for broker " + host + ":" + port);
        return;
    }
    
    System.out.println("List of topics for broker " + this.getHost() + ":" + this.getPort() + ":");
    
    for (int i = 0; i < topics.size(); i++) {
        KafkaTopic topic = topics.get(i);
        System.out.println("Topic " + (i + 1) + ": " + topic.getName()); //(i) or (i+1) in 'System.out.println' is chosen according to the SampleOutput.txt
        
        System.out.println(" Partitions:");
        ArrayList<KafkaPartition> partitions = topic.getPartitions();
        
        for (KafkaPartition partition :partitions) {
            
            if (partition != null) {
                System.out.println("  Partition " + (partitions.indexOf(partition)+1) + ":");
                System.out.println("    Replicas:");
                ArrayList<KafkaReplica> replicas = partition.getReplicas();
                
                for (KafkaReplica replica: partition.getReplicas()) {
                    
                    if (replica != null) {
                        KafkaBroker broker = replica.getBroker();
                        System.out.println("     Replica " + (replicas.indexOf(replica)+1) + ": " + broker.getHost() + ":" + broker.getPort());
                    }
                }
            }
        }
    }
}



@Override
public String toString(){
    String toBeReturned = "Broker host = " + this.host + "\t" + "port = " + this.port + "\t" + "max topics = " + this.maxTopics+ "\t" +"current numberber of topics = " + this.topics.size() +"\n";
    for (KafkaTopic kt: topics){
        if (kt == null) break;
        toBeReturned += kt.toStringTopicAllinfo() + "\n";
    }
    return toBeReturned;
}

}














