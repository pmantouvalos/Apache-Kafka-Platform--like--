package oopproject1.admin;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
//import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
*
* @author pmantouvalos,dzoidakis
*/


public class KafkaCluster {
                                 
private ArrayList<KafkaBroker> brokers; //we can use this variables as static because we have only one instance of KafkaCluster
private int brokerCount;       
private int maxBrokers;         
private static final int DEFAULT_MAX_BROKERS=5;
private int maxTopicsPerBroker; // maxTopicsPerBroker variable is used to replace when is needed the brokers[i].getMaxTopics() expression


//Constructor with validation

public KafkaCluster(int maxBrokers, int maxTopicsPerBroker) throws IllegalArgumentException{
   
	if(!(KafkaTopic.checkValidPositiveInteger(maxBrokers))) {
		throw new IllegalArgumentException("The maximum number of brokers cannot be negative or zero");
	}

	if(!(KafkaTopic.checkValidPositiveInteger(maxTopicsPerBroker))) {
		throw new IllegalArgumentException("The maximum number of topics per broker cannot be negative or zero");
	}
	this.brokers=new ArrayList<KafkaBroker>(maxBrokers);
	this.maxTopicsPerBroker=maxTopicsPerBroker;
	this.brokerCount=0;
	this.maxBrokers=maxBrokers;
	
}


public static KafkaCluster init() {
	int maxBrokersInCluster =10;
	int maxTopicsPerBroker = 10;
	KafkaCluster cluster = new KafkaCluster(maxBrokersInCluster, maxTopicsPerBroker);
	try {
		for (int i = 0; i < maxBrokersInCluster; i++) {
			cluster.insertBroker(new KafkaBroker("127.0.0.1", 9090 + i, 10));
}
		
		KafkaTopic[] topics = new KafkaTopic[6];
		cluster.addTopic("stock_prices", 30, 50, 20, 4, true);
		topics[0] = cluster.findTopicByName("stock_prices");
		
		cluster.addTopic("ship_locations", 20, 7, 12, 3, true);
		topics[1] = cluster.findTopicByName("ship_locations");
		
		cluster.addTopic("power_consumptions", 30, 33, 33, 4, false);
		topics[2] = cluster.findTopicByName("power_consumptions");
		
		cluster.addTopic("weather_conditions", 4, 3, 7, 2, true);
		topics[3] = cluster.findTopicByName("weather_conditions");
		
		cluster.addTopic("plane_locations", 6, 12, 12, 2, true);
		topics[4] = cluster.findTopicByName("plane_locations");
		
		cluster.addTopic("bookings", 4, 2, 2, 1, false);
		topics[5] = cluster.findTopicByName("bookings");
				
		
		for (int i = 0; i < topics.length; i++) {
			if (topics[i]==null) {
				continue;
			}
			for (int j = 0; j < topics[i].getMaxProducers(); j++) {
				new KafkaProducer(topics[i]);
			}
			for (int j = 0; j < topics[i].getMaxConsumers(); j++) {
					new KafkaConsumer(topics[i]);
			}
		}
	} catch (Exception e) {
		System.out.println("Cluster initialization failed. Error: " + e.getMessage());
		// e.printStackTrace();
	}
	return cluster;
}


//Getters and Setters with validation (All the expressly mentioned unnecessary setters/getters were removed) 


protected void setBrokerCount(int brokerCount) throws IllegalArgumentException {
	if(!KafkaTopic.checkValidPositiveInteger(brokerCount)) {
	throw new IllegalArgumentException("Broker count cannot be negative ");
	}	
		this.brokerCount = brokerCount;
	
	}
	

protected void setMaxTopicsPerBroker(int maxTopicsPerBroker) throws IllegalArgumentException {
	if(!KafkaTopic.checkValidPositiveInteger(maxTopicsPerBroker)) {
	throw new IllegalArgumentException("MaxTopicsPerBroker cannot be negative");
	}	
		this.maxTopicsPerBroker = maxTopicsPerBroker;
	
	}



protected void setBrokers(ArrayList<KafkaBroker> brokers)throws IllegalArgumentException  {
	if(KafkaTopic.checkNullObject(brokers)||brokers.size()==maxBrokers) {
		throw new IllegalArgumentException("Brokers arrayList cannot be null and the number of brokers contained must be smaller or equal to maxBrokers");	
	}
	
	this.brokers = brokers;
}



protected void setMaxBrokers(int maxBrokers) throws IllegalArgumentException  {
	if(!KafkaTopic.checkValidPositiveInteger(maxBrokers)) {
	throw new IllegalArgumentException(" maxBrokers cannot be negative");
	}	
		this. maxBrokers =  maxBrokers;
	
	}



protected ArrayList<KafkaBroker> getBrokers() {
	return brokers;
}


protected int getMaxBrokers() {
	return maxBrokers;
}

protected  int getBrokersSize() {
	return brokers.size();
}

protected int getBrokerCount() {
	return brokerCount;
}

protected int getMaxTopicsPerBroker() {
	return maxTopicsPerBroker;
}

protected static int getDefaultMaxBrokers() {
	return DEFAULT_MAX_BROKERS;
}



//------------------------methods----------------------



public void insertBroker(KafkaBroker broker) throws Exception {
	
	if(KafkaTopic.checkNullObject(broker)) {
		throw new NullPointerException("Kafka broker cannot be null");
	}
	
	if (brokers.size() == maxBrokers) {
        throw new IllegalStateException("Cannot insert more brokers. Cluster is full of brokers.");
    
	} 
 	
	if(checkBrokerExistence(broker.getHost(),broker.getPort())) {
	throw new IllegalStateException("Broker with the same host and port already exists.");	
	}
	
	brokers.add(broker);
	this.brokerCount++;  //we are just taking tracking of brokers amount in our cluster except for brokers.size()
}

/*in this method, at first, we have to delete all the topics, partitions and replicas belonging to the given broker 
 * and then we will remove the broker.For this reason we will use the deleteTopic method from this class*/

public void removeBroker(String host, int port) throws IllegalArgumentException {
    if (!checkBrokerExistence(host, port)) {
        throw new IllegalArgumentException("Broker with host " + host + " and port " + port + " does not exist.");
    }

    Iterator<KafkaBroker> brokerIterator = brokers.iterator();
    while (brokerIterator.hasNext()) {
        KafkaBroker brok = brokerIterator.next();
        if (brok != null && brok.getHost().equals(host) && brok.getPort() == port) {
            
            List<KafkaTopic> topicsToDelete = new ArrayList<>(brok.getTopics());
            for (KafkaTopic t : topicsToDelete) {
                if (t != null) {
                    deleteTopic(t.getName());
                }
            }
            brokerIterator.remove(); 
            this.brokerCount--;
            System.out.println("Broker " + host + ":" + port + " and its associated topics, partitions, and replicas removed successfully.");
            break;
        }
    }
}



//through this method we delete a topic but before that,we delete its partitions and its replicas from the other brokers
public void deleteTopic(String topicName) throws IllegalArgumentException {
    KafkaTopic topic = findTopicByName(topicName);

    if (topic == null) {
        throw new IllegalArgumentException("Topic " + topicName + " not found in the Kafka cluster.");
    }

    boolean topicFound = false;

    Iterator<KafkaBroker> brokerIterator = brokers.iterator();
    while (brokerIterator.hasNext()) {
        KafkaBroker broker = brokerIterator.next();
        if (checkBrokerExistence(broker.getHost(), broker.getPort())) {
            Iterator<KafkaTopic> topicIterator = broker.getTopics().iterator();
            while (topicIterator.hasNext()) {
                KafkaTopic currentTopic = topicIterator.next();
                if (currentTopic.getName().equals(topicName)) {
                    
                	
                	
                	
                	Iterator<KafkaPartition> partitionIterator = broker.getTopics().get(broker.getTopics().indexOf(currentTopic)).getPartitions().iterator();
                	while(partitionIterator.hasNext()) {
                	     KafkaPartition partition = partitionIterator.next();
                		if (partition != null) {
                            Iterator<KafkaReplica> replicaIterator = partition.getReplicas().iterator();
                            while (replicaIterator.hasNext()) {
                                 
                            	 replicaIterator.next();
                                 replicaIterator.remove();
                                 partition.setReplicaCount(partition.getReplicas().size());
                            }
                            partitionIterator.remove();; // Διαγραφή του partition
                        }
                    }
                    topicIterator.remove(); // Διαγραφή του topic από τον broker
                    topicFound = true;
                    break;
                }
            }
        }
    }

    if (!topicFound) {
        throw new IllegalArgumentException("Topic " + topicName + " not found in the Kafka cluster.");
    } else {
        System.out.println("Topic " + topicName + " and all related partitions and replicas deleted successfully.");
    }
}

//it finds a host that has the given host AND port 
public KafkaBroker findBrokerByHostAndPort(String host ,int port) {
	
	if (checkBrokerExistence(host, port)) {
        for (KafkaBroker broker : brokers) {
            if (broker != null && broker.getHost().equals(host) && broker.getPort() == port) {
                return broker;
            }
        }
    }
    
    return null;
}

//through this function we update a broker's port after all the necessary checks 
public void updateBrokerPort(String host, int port, int newPort) throws IllegalArgumentException {
    
	
		KafkaBroker brokerToUpdate = findBrokerByHostAndPort(host, port);	
	
	if (brokerToUpdate == null) {
        throw new IllegalArgumentException("Broker not found.");
    }
 
    if (brokerToUpdate.getPort() == newPort) {
        throw new IllegalArgumentException("Broker has already this port.");
    }

    for (KafkaBroker otherBroker : brokers) {
        if (otherBroker!=null && otherBroker != brokerToUpdate && otherBroker.getPort() == newPort) {
            throw new IllegalArgumentException("Port " + newPort + " is already occupied.");
            }
          }
             brokerToUpdate.setPort(newPort);
        
}

public String reportBroker(String host,int port){
        KafkaBroker brokerx = this.findBrokerByHostAndPort(host,port);
        if (brokerx != null)
            return brokerx.toString();
        return null;
    }


public String reportTopic(String topicName){
        KafkaTopic topicx = this.findTopicByName(topicName);
        if (topicx != null)
            return topicx.toStringTopicAllinfo();
        return null;
    }

public void addTopic(String topicName, int numPartitions, int maxProducers, int maxConsumers, int replicationFactor, boolean keyed) throws Exception {
	
	if(this.checkTopicExistence(topicName)) {
		 throw new IllegalArgumentException("Topic with name '" + topicName + "' already exists in the cluster");
	}
	
	if(!(replicationFactor<=this.getBrokersSize())) {
		 throw new IllegalArgumentException("Replication factor cannot be greater than the number of brokers.");
	}
	
	KafkaBroker brokerWithMinimumTopics = brokers.get(0);
	
	KafkaBroker owner;
	//checks in order to find the correct broker to add the topic, according to the given description of the topic class
	//first we want to find the broker with minimum topics added
	for(int i =0 ; i< brokers.size();i++) {
		if(brokers.get(i).getTopicsSize()<brokerWithMinimumTopics.getTopicsSize()) {
			brokerWithMinimumTopics=brokers.get(i);
		}
	}
	
	owner = brokerWithMinimumTopics; 
   
	//if two or more brokers have the same topicCount,we want to add the topic into the broker with the smaller Id
	for (KafkaBroker broker : brokers) {
	    if (broker!=null && broker.getTopicsSize() == brokerWithMinimumTopics.getTopicsSize()) {
	    	if (broker.getBrokerId() < owner.getBrokerId()) {
	            owner = broker; 
	        }
	    }
	}
		
	KafkaTopic topicToAdd=new KafkaTopic(topicName,numPartitions,owner,maxProducers, maxConsumers, replicationFactor,keyed);
	
	owner.addTopic(topicToAdd);
    
	for(int i=0;i<numPartitions;i++) {
		topicToAdd.getPartitions().add(new KafkaPartition(replicationFactor));
	}
	
	ArrayList<KafkaPartition> partitions = topicToAdd.getPartitions();
	
	KafkaReplica replica;
	
	for (KafkaPartition partition : partitions) {
		if(partition!=null) {
		
			for(int replicasAdded=0;replicasAdded<replicationFactor;) {
			for(KafkaBroker broker: brokers) {
				if(replicasAdded<replicationFactor) {
				if(!KafkaTopic.checkNullObject(broker)) {
			if(broker!=owner) {
				replica =new KafkaReplica(broker,partition);
				partition.setReplicaCount(partition.getReplicas().size());
				replicasAdded++;
				partition.addReplica(replica);
			}
		 }
		
	 } 
    }
   }
  }
  }
 }


public KafkaTopic findTopicByName(String topicName){
	
	/*if a topiName is found in one broker, then we can claim that we have found the topic that we are searching 
	 *and that's because there is not a topic with the same name in two different brokers (see the given data for the KafkaTopic class)
	 */
		
	if (checkTopicExistence(topicName)) {
       for (KafkaBroker broker : brokers) {
           if (broker != null) {
               for (KafkaTopic topic : broker.getTopics()) {
                  if (topic != null && topic.getName().equals(topicName)) {
                     return topic;
                        }
                      }
                    }
                  }
                }
         return null;
       }


//method to list all brokers
public void listAllBrokers() {
	
    System.out.println("List of Brokers:");
	
    for(int i=0;i<brokers.size();i++) {
	   System.out.println("Broker "+i+":");
	   System.out.println("Host: "+brokers.get(i).getHost());
	   System.out.println("Port: "+brokers.get(i).getPort());
	   System.out.println("Topic Count: "+brokers.get(i).getTopicsSize());
	   System.out.println();	
	   
		
    }

}

public void listOnlyTopics() {
    int i=0;
    System.out.println("Here are all the available topics that you can write messages to:");
    System.out.println();
    for(KafkaBroker broker: this.brokers) {
        for(KafkaTopic topic:broker.getTopics()) {
            System.out.println("Topic "+(i+1)+": "+topic);
            i++;
        }
            
    }
}


public ArrayList<KafkaTopic> getAllTopics(){
    
    ArrayList<KafkaTopic> allTopics =new ArrayList<KafkaTopic>();
    
    
    for(int k=0;k<brokers.size();k++) {
		   
		 for(int i=0;i<brokers.get(k).getTopics().size(); i++) {
		if(brokers.get(k).getTopics().get(i)!=null) {
		allTopics.add(brokers.get(k).getTopics().get(i));
		}
	}
	}
    
    return allTopics;
}




//method to list all brokers and except for that, to list all of their topics without informations about them
public void listAllTopicsAcrossBrokers() {
	
	System.out.println("List of Topics Across Brokers:");
	
	for(int i=0;i<brokers.size();i++) {
		   System.out.println("Broker "+i+":");
		   System.out.println("Host: "+brokers.get(i).getHost());
		   System.out.println("Port: "+brokers.get(i).getPort());
		   System.out.println("List of Topics for broker "+brokers.get(i).getHost()+":"+brokers.get(i).getPort());
		   brokers.get(i).listAllTopics();
	}
		
}
//the same with the namesake method,but now we print topic's informations too
public void listAllTopicsAcrossBrokers(boolean includeDetails) {
	
	System.out.println("List of Topics Across Brokers:");
	
	for(int i=0;i<brokers.size();i++) {
		   System.out.println("Broker "+i+":");
		   System.out.println("Host: "+brokers.get(i).getHost());
		   System.out.println("Port: "+brokers.get(i).getPort());
		   brokers.get(i).listAllTopics(includeDetails);

}	
}

//this function returns true if broker exists in cluster and false if it doesn't exist
//it can be called both as static and as non-static because we have only one instance of cluster
public boolean checkBrokerExistence(String host,int port) throws IllegalArgumentException {
	
	if(!KafkaBroker.isValidHost(host)) {
		throw new IllegalArgumentException("Invalid host format.");
	}
	
	if(!KafkaBroker.isValidPort(port)) {
		throw new IllegalArgumentException("Port must be greater than 0 and smaller than 65535.");
	}
	for(KafkaBroker brok: brokers) {
		if(brok!=null &&brok.getHost().equals(host) && brok.getPort()==port) {
			return true;
		}
	}
	
	return false;
	
	}

//this function returns true if topic exists in cluster and false if it doesn't exist
public boolean checkTopicExistence(String topicName) throws IllegalArgumentException{
    if (!KafkaTopic.checkString(topicName)) {
    	 throw new IllegalArgumentException("Topic's name cannot be null or empty.");
    }

    for (KafkaBroker broker : brokers) {
        if (broker != null) {
            for (KafkaTopic topic : broker.getTopics()) {
                if (topic != null && topic.getName().equals(topicName)) {
                    return true; 
                }
            }
        }
    }

    return false; 
 }


public void readAndSaveAll() throws FileNotFoundException, IOException {
	
	ObjectOutputStream os = null;
	FileOutputStream fos = null;

	try {
		
		
		for(KafkaBroker kb:this.brokers) {
			if(!KafkaTopic.checkNullObject(kb)) {
			for(KafkaTopic kt:kb.getTopics()) {
				if(!KafkaTopic.checkNullObject(kt)) {
				for(KafkaConsumer kc:kt.getConsumers() ) {
					kc.receiveMessage();
				}
				
				LinkedStack<Message> stack = kt.getMessageStorager();
				
				if(!kt.getMessageStorager().isEmpty()) {
					try {
					fos = new FileOutputStream("messages"+kt.toString()+".txt");
					os = new ObjectOutputStream(fos);
					
					while(!stack.isEmpty()){
                                            os.writeObject(stack.pop());
                                        }
					
					
					
						os.close();
						fos.close();
						
					}catch(Exception e){
					}
					
				}
			}
			}
		}
		}
			
		
		System.out.println("All messages are stored successfully ....");
	}finally {
		try{
			os.close();
			fos.close();
		}catch(Exception e){
		}

	}
	
	
	
}
	
}
	

/*public void saveToFile() throws IOException {
	for(KafkaBroker kb:this.brokers) {
		for(KafkaTopic kt:kb.getTopics()) {
			if(!KafkaTopic.checkNullObject(kt)) {
			  if(kt.getMessageStorager().isSerializable()==true){
				String fileName = "broker_"+kb.getHost()+":"+kb.getPort()+"_topic_"+kt.getName()+".stack";
				String filePath= "C:\\Users\\dnzoi\\Downloads\\"+fileName;
                try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filePath))) {
                    while(!kt.getMessageStorager().isEmpty()) {
                	
                	out.writeObject(kt.getMessageStorager());
                	
                    }
                    System.out.println("The message stack/s  was/were serialized  successfully.");
                } catch (IOException e) {
                 e.printStackTrace();
                }
				
			
			}
			}
		}
		}
	
}*/

/*public void saveToFile() throws IOException {
	
	for(KafkaBroker kb:this.brokers) {
		for(KafkaTopic kt:kb.getTopics()) {
			if(!KafkaTopic.checkNullObject(kt)) {
			  if(kt.getMessageStorager().isSerializable()==true){
				String fileName = "broker_"+kb.getHost()+":"+kb.getPort()+"_topic_"+kt.getName()+".txt";
				String filePath= "C:\\Users\\dnzoi\\Downloads\\"+fileName;
				
				
				File file = new File(filePath);
				 try {
				 if (file.createNewFile()) {
				 System.out.println("File is created !!");
				 } else {
				 System.out.println("File already exists");
				 }
				 } catch (IOException e) {
				 System.out.println(e.getMessage());
				 }
				 
				
                try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file))) {
                    while(!kt.getMessageStorager().isEmpty()) {
                	
                	out.writeChars(kt.getMessageStorager().pop().toString());
                	
                    }
                    out.close();
                    System.out.println("The message stack/s  was/were serialized  successfully.");
                } catch (IOException e) {
                 e.printStackTrace();
                }
				
			
			}
			}
		}
		}
	
}

*/



/*public void saveToFile() throws IOException {
	FileWriter outStream = null;
	File file= new File("C:\\Users\\dnzoi\\Downloads\\LinkedStacks");
	
	for(KafkaBroker kb:this.brokers) {
		for(KafkaTopic kt:kb.getTopics()) {
			if(!KafkaTopic.checkNullObject(kt)) {
			  if(kt.getMessageStorager().isSerializable()==true){
				String fileName = "broker_"+kb.getHost()+":"+kb.getPort()+"_topic_"+kt.getName()+".txt";
				String filePath= "C:\\Users\\dnzoi\\Downloads\\LinkedStacks\\"+fileName;
				
                try {
                	
                	
                	
                	if (!file.exists() && file.getParentFile().mkdirs()) {
            			file.createNewFile();
            		}
            		if (!file.exists()) {
            			throw new FileNotFoundException("File " + fileName+ " does not exist");
            		}
                    while(!kt.getMessageStorager().isEmpty()) {
                    	outStream = new FileWriter(file);
            			outStream.write(kt.getMessageStorager().pop().toString());
            			outStream.close();
                	
                	
                    }
                    System.out.println("The message stack/s  was/were serialized  successfully.");
                }  catch (Exception ex) {
        			System.out.println("Could not store to file. Error = " + ex.getMessage());
        		} finally {
        			if (outStream != null)
        				try {
        					outStream.close();
        				} catch (Exception ex) {
        				}
        		}
		}
		}
	}
}

}*/




