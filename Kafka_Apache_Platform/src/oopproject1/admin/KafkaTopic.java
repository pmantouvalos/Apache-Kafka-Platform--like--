package oopproject1.admin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
*
* @author pmantouvalos,dzoidakis
*/

public class KafkaTopic{

private String name;
private KafkaBroker owner;
private ArrayList <KafkaPartition> partitions;
private ArrayList<KafkaProducer> producers;
private ArrayList<KafkaConsumer> consumers;
private int replicationFactor;
private boolean keyed;
private int maxProducers;
private int maxConsumers;
private int numPartitions;
private static final int DEFAULT_REPLICATION_FACTOR=1;
private static final boolean DEFAULT_KEYED= false;
private static final int DEFAULT_MAX_PRODUCERS=10;
private static final int DEFAULT_MAX_CONSUMERS=10;

private LinkedStack <Message> messageStorager;

//Constructor with validation
protected KafkaTopic(String name, int numPartitions, KafkaBroker owner, int maxProducers, int maxConsumers, int replicationFactor, boolean keyed) throws IllegalArgumentException{
	
	if(!checkString(name)) {
		 throw new IllegalArgumentException("Topic's name cannot be null or empty.");
	}
		
	if(!(checkValidPositiveInteger(numPartitions) && numPartitions>=1)) {
		 throw new IllegalArgumentException("Number of partitions must be greater or equal to one");
	}
	
	if(!(checkValidPositiveInteger(maxProducers))) {
		 throw new IllegalArgumentException("Number of maximum producers must be positive");
	}
	this.maxProducers=maxProducers;
	
	if(!checkValidPositiveInteger(maxConsumers)) {
		 throw new IllegalArgumentException("Number of maximum consumers must be positive");
	}
	this.maxConsumers=maxConsumers;
	
	if(!(checkValidPositiveInteger(replicationFactor))) {
		 throw new IllegalArgumentException("Replication factor cannot be greater than the number of brokers");
	}
	
	this.name=name;
    this.owner=owner;
    this.partitions= new ArrayList <KafkaPartition>(numPartitions);
    this.numPartitions=numPartitions;
    this.producers= new ArrayList<KafkaProducer>(maxProducers);
    this.consumers= new ArrayList<KafkaConsumer>(maxConsumers);
    this.replicationFactor=replicationFactor;
    this.keyed=keyed;
    messageStorager= new LinkedStack<>();
    
    
}

//Overloaded constructor with default replication factor and keyed values

private KafkaTopic(String name, int numPartitions, KafkaBroker owner, int maxProducers, int maxConsumers){
	this(name, numPartitions, owner, maxProducers, maxConsumers, DEFAULT_REPLICATION_FACTOR, DEFAULT_KEYED);

}

//Overloaded constructor with default max producers and consumers

private KafkaTopic(String name, int numPartitions, KafkaBroker owner, int replicationFactor, boolean keyed){
	this(name, numPartitions, owner, DEFAULT_MAX_PRODUCERS, DEFAULT_MAX_CONSUMERS, replicationFactor, keyed);

}

//Overloaded constructor with default replication factor, keyed, max producers, and max consumers

private KafkaTopic(String name, int numPartitions, KafkaBroker owner){
	this(name, numPartitions, owner, DEFAULT_MAX_PRODUCERS, DEFAULT_MAX_CONSUMERS, DEFAULT_REPLICATION_FACTOR, DEFAULT_KEYED);

}


//Getters and Setters with validation (All the expressly mentioned unnecessary setters/getters were removed)

protected void setMaxProducers(int maxProducers) throws IllegalArgumentException{
	
	if(!(checkValidPositiveInteger(maxProducers))) {
		 throw new IllegalArgumentException("Number of maximum producers must be positive");
	}
	
	this.maxProducers = maxProducers;
}

protected void setMaxConsumers(int maxConsumers) throws IllegalArgumentException{
	
	if(!checkValidPositiveInteger(maxConsumers)) {
		 throw new IllegalArgumentException("Number of maximum consumers must be positive");
	}
		
	this.maxConsumers = maxConsumers;
}

protected void setPartitions(ArrayList<KafkaPartition> partitions) throws IllegalArgumentException{
	
	if((!(checkValidPositiveInteger(partitions.size()) && partitions.size()>numPartitions))|| KafkaTopic.checkNullObject(partitions)) {
		 throw new IllegalArgumentException("Number of partitions must be greater or equal to one");
	}
	
	this.partitions = partitions;
}

protected void setProducers(ArrayList<KafkaProducer> producers) throws IllegalArgumentException {
	
	
	if((!(checkValidPositiveInteger(producers.size()) && producers.size()>maxProducers))|| KafkaTopic.checkNullObject(producers)) {
		 throw new IllegalArgumentException("Producers size must be valid positive integer and less than maxProducers");
	}
	
	this.producers = producers;
}

protected void setConsumers(ArrayList<KafkaConsumer> consumers) throws IllegalArgumentException{
	
	if((!(checkValidPositiveInteger(consumers.size()) && consumers.size()>maxConsumers))||KafkaTopic.checkNullObject(consumers)) {
		 throw new IllegalArgumentException("Consumers size must be valid positive integer and less than maxConsumers");
	}
	
	this.consumers = consumers;
}


protected void setMessageStorager(LinkedStack<Message> messageStorager) {
	this.messageStorager = messageStorager;
}

public String getName() {
	return name;
}

public KafkaBroker getOwner() {
	return owner;
}

protected ArrayList <KafkaPartition> getPartitions() {
	return partitions;
}

public ArrayList<KafkaProducer> getProducers() {
	return producers;
}

public ArrayList<KafkaConsumer> getConsumers() {
	return consumers;
}

public int getReplicationFactor() {
	return replicationFactor;
}

public boolean getKeyed() {
	return keyed;
}

public int getNumPartitions() {
	return this.numPartitions;
}
protected LinkedStack<Message> getMessageStorager() {
	return messageStorager;
}

public int getMaxProducers() {
	return maxProducers;
}

public int getMaxConsumers() {
	return maxConsumers;
}


protected static int getDefaultReplicationFactor() {
	return DEFAULT_REPLICATION_FACTOR;
}

protected static boolean isDefaultKeyed() {
	return DEFAULT_KEYED;
}

protected static int getDefaultMaxProducers() {
	return DEFAULT_MAX_PRODUCERS;
}

protected static int getDefaultMaxConsumers() {
	return DEFAULT_MAX_CONSUMERS;
}
































//--------------------------checkers---------------------------------


//returns false for null or empty string
public static boolean checkString(String name) {
	return (!checkNullObject(name)) && !name.trim().isEmpty();
}


//with this method we check if an object is null
protected static boolean checkNullObject(Object obj) {
	return obj == null;
}

//------------------------methods---------------------------------

//method to add producers in topic's producers array
public void addProducer(KafkaProducer producer) throws IllegalStateException {
	  if (producers.size() < this.maxProducers) {
          producers.add(producer);
      } else {
        throw new IllegalStateException("Cannot add more producers. Topic is full of producers.");
      }
}



//method to add consumers in topic's producers array
public void addConsumer(KafkaConsumer consumer) throws IllegalStateException {
	  if (consumers.size() < this.maxConsumers) {
        consumers.add(consumer);
       
       
    } else {
      throw new IllegalStateException("Cannot add more consumers. Topic is full of consumers.");
    }
}




/*method to remove producers from topic's producers array,it is not mentioned in the asked methods section, 
 * but is described at class description 
 */
public void removeProducer(KafkaProducer producer) throws IllegalArgumentException {
    boolean producerFound = false;
    for (int i = 0; i < producers.size(); i++) {
        if (producers.get(i).equals(producer)) {
            producers.remove(i);
            producerFound = true;
            System.out.println("Producer " + producer + " removed.");
            break;
        }
    }
    if (!producerFound) {
        throw new IllegalArgumentException("Producer not found.");
    }
}

/*method to remove consumers from topic's consumers array,it is not mentioned in the asked methods section, 
 * but is described at class description 
 */


public void removeConsumer(KafkaConsumer consumer) throws IllegalArgumentException {
    boolean consumerFound = false;
    for (int i = 0; i < consumers.size(); i++) {
        if (consumers.get(i).equals(consumer)) {
            consumers.remove(i);
            consumerFound = true;
            System.out.println("Consumer " + consumer + " removed.");
            break;
        }
    }
    if (!consumerFound) {
        throw new IllegalArgumentException("Consumer not found.");
    }
}


/*through this method we check if the given numbers are valid integers
 * it is static because we want to call it from other classes, without creating a KafkaTopic object each time
 * return type is boolean so we can use it in if statements and make our code DRY
 * Source for Integer.MAX_VALUE: web
 */

public static boolean checkValidPositiveInteger(int parameter) {
   	return parameter > 0 && parameter <= Integer.MAX_VALUE;
}
            
 
@Override
public String toString(){
	return this.getName();
}



public static List<List<Integer>> distributePartitions(int numPartitions, int numConsumers) {
    List<List<Integer>> partitionRanges = new ArrayList<>();
    if (numPartitions >= numConsumers) {
        // Calculate the number of partitions each consumer should ideally handle
        int partitionsPerConsumer = numPartitions / numConsumers;
        int remainder = numPartitions % numConsumers; // Handle the case when numPartitions is not evenly divisible by numConsumers
        // Initialize the starting partition index
        int startPartition = 0;
        // Distribute partitions to consumers
        for (int i = 0; i < numConsumers; i++) {
            int endPartition = startPartition + (remainder-- > 0 ? partitionsPerConsumer : partitionsPerConsumer - 1);
            
            List<Integer> partitionRange = new ArrayList<>();
            partitionRange.add(startPartition);
            partitionRange.add(endPartition);
            partitionRanges.add(partitionRange);
            // Update the starting partition index for the next consumer
            startPartition = endPartition + 1;
            // Wrap around if the endPartition exceeds the total number of partitions
            if (startPartition >= numPartitions) {
                startPartition %= numPartitions;
            }
    }
    } 
    else {
        // There are more consumers than partitions
    // Some consumers will receive one partition and the rest will receive none
        for (int i = 0; i < numConsumers; i++) {
            List<Integer> partitionRange = new ArrayList<>();
            partitionRange.add(i < numPartitions ? i : -1);
            partitionRange.add(i < numPartitions ? i : -1);
            partitionRanges.add(partitionRange);
        }
    }
return partitionRanges;
}


public static List<List<Integer>> distributeMessageRangesToProducers(int numOfRecords, int numProducers) {
    List<List<Integer>> listOfRecordRanges = new ArrayList<>();
    if (numOfRecords >= numProducers) {
        // Calculate the number of partitions each consumer should ideally handle
        int recordsPerProducer = numOfRecords / numProducers;
        int remainder = numOfRecords % numProducers; // Handle the case when numPartitions is not evenly divisible by numConsumers
        // Initialize the starting partition index
        int startRecord = 0;
        // Distribute partitions to consumers
        for (int i = 0; i < numProducers; i++) {
            int endRecord = startRecord + (remainder-- > 0 ? recordsPerProducer : recordsPerProducer - 1);
            
            List<Integer> recordsRange = new ArrayList<>();
            recordsRange.add(startRecord);
            recordsRange.add(endRecord);
            listOfRecordRanges.add(recordsRange);
            // Update the starting partition index for the next consumer
            startRecord = endRecord + 1;
            // Wrap around if the endPartition exceeds the total number of partitions
            if (startRecord >= numOfRecords) {
            	startRecord %= numOfRecords;
            }
    }
    } 
    else {
        // There are more consumers than partitions
    // Some consumers will receive one partition and the rest will receive none
        for (int i = 0; i < numProducers; i++) {
            List<Integer> partitionRange = new ArrayList<>();
            partitionRange.add(i < numOfRecords ? i : -1);
            partitionRange.add(i < numOfRecords ? i : -1);
            listOfRecordRanges.add(partitionRange);
        }
    }
return listOfRecordRanges;
}


public void readFirstLine(String fileName) throws IOException{
	File file = new File(fileName);

    if (!file.exists()) {
        throw new FileNotFoundException("File " + fileName + " does not exist");
    }
    BufferedReader fileReader = null;
    try {
        fileReader = new BufferedReader(new FileReader(file));
        
        System.out.println(fileReader.readLine());
    }
    finally {
        if (fileReader != null) {
            fileReader.close();
        }
    
    }
	
}





public void fileReader(String fileName) throws IOException,Exception {
	
	File file = new File(fileName);

	BufferedReader fileReader = null;
	
try {
    if (!file.exists()) {
        throw new FileNotFoundException("File " + fileName + " does not exist");
    }

	fileReader = new BufferedReader(new FileReader(file));
	int recordCounter=0;
	while(fileReader.readLine() != null) {recordCounter++;}
	fileReader.close();
	
	fileReader = new BufferedReader(new FileReader(file));
	fileReader.readLine();
	//και κατανομή και διάβασμα

	
	int recordIndexRangeFirst;//start position of partition at the range set
	int recordIndexRangeLast;//end position of partition at the range set

	for(int k = 0; k <this.producers.size();k++)
	{
	 


	List<Integer> listWithRecordRange = KafkaTopic.distributeMessageRangesToProducers(recordCounter,this.producers.size()).get(k);



	recordIndexRangeFirst = listWithRecordRange.get(0);
	recordIndexRangeLast = listWithRecordRange.get(1);

	    
	if ( recordIndexRangeFirst == -1 && recordIndexRangeLast == -1){
		System.out.println("Consumer "+k+" of 'topic "+this+"' isn't assigned any partitions");
		
	}else{
		if ( recordIndexRangeFirst == recordIndexRangeLast){
			String line;
            while ((line = fileReader.readLine()) != null) {
                String value = line.trim();
                producers.get(k).sendMessage(value);
            }
		                                                                                                
		
	   	}else{   

			for (int i = recordIndexRangeFirst; i <= recordIndexRangeLast; i++) {
				 String line;
	                while ((line = fileReader.readLine()) != null) {
	                    String value = line.trim();
	                    producers.get(k).sendMessage(value);
	                }
	            }                                                                                                
				}
		
			}
		}
	fileReader.close();
	
		System.out.println();
        
 	   System.out.println((recordCounter-1)+" messages are loaded!");
} finally {
    if (fileReader != null) {
        fileReader.close();
    }
}
}


public void fileReader(String fileName, int keyIndex) throws FileNotFoundException, Exception {
    File file = new File(fileName);

  //if (!file.exists()) {
  //      throw new FileNotFoundException("File " + fileName + " does not exist");
  // }
  //ελεγχος που γινεται στην CLI
    BufferedReader fileReader = null;
    
        fileReader = new BufferedReader(new FileReader(file));
        int recordCounter = 0;
        while (fileReader.readLine() != null) {
            recordCounter++;
        }
        fileReader.close();
        try {
        fileReader = new BufferedReader(new FileReader(file));
        fileReader.readLine(); // Skip the first line

        int recordIndexRangeFirst;
        int recordIndexRangeLast;

        for(int k = 0; k <this.producers.size();k++){
    	List<Integer> listWithRecordRange = KafkaTopic.distributeMessageRangesToProducers(recordCounter-1,this.producers.size()).get(k);//δεν προσμετρώ ως record την πρώτη γραμμή του αρχείου



    	recordIndexRangeFirst = listWithRecordRange.get(0);
    	recordIndexRangeLast = listWithRecordRange.get(1);

    	    
    	if ( recordIndexRangeFirst == -1 && recordIndexRangeLast == -1){
    		System.out.println("Producer "+k+" of 'topic' "+this+" isn't assigned any partitions");
    		
    	}else{
    		if ( recordIndexRangeFirst == recordIndexRangeLast){
    			 String line;
                 if ((line = fileReader.readLine()) != null) {
                     String[] parts = line.split(",");
                     String key = parts[keyIndex].trim();
                     StringBuffer value = new StringBuffer();
                     for (int j = 0; j < parts.length; j++) {
                         if (j != keyIndex) {
                             if (value.length() > 0) {
                                 value.append(",");
                             }
                             value.append(parts[j].trim());
                         }
                     }
                     producers.get(k).sendMessage(key, value.toString());
                 }
    		                                                                                                
    		
    	   	}else{   

    			for (int i = recordIndexRangeFirst; i <= recordIndexRangeLast; i++) {
    				 String line;
    	                if ((line = fileReader.readLine().trim()) != null) {
    	                    String[] parts = line.split(",");
    	                    String key = parts[keyIndex].trim();
    	                    StringBuffer value = new StringBuffer();
    	                    for (int j = 0; j < parts.length; j++) {
    	                        if (j != keyIndex) {
    	                            if (value.length() > 0) {
    	                                value.append(",");
    	                            }
    	                            value.append(parts[j]);
    	                        }
    	                    }
    	                    producers.get(k).sendMessage(key, value.toString());
    	                	}  
    				}
    	   	}
    		
    
    		
    	}
        }
        
        
        System.out.println();
        
         System.out.println((recordCounter-1)+" messages are loaded!");
        } finally {
            if (fileReader != null) {
                fileReader.close();
            }
        }

}




protected String toStringTopicAllinfo(){
    String toBeReturned = "Topic's name = " + this.name + "\t" + "number of partitions = " + this.numPartitions + "\t" + "owner broker = " + this.owner+ "\t" +"max number of producers = " 
    + this.maxProducers + "\t" + "max number of consumers = " + this.maxConsumers + "\t" + "replication factor = " + this.replicationFactor+ "isKeyed = " +this.keyed
    + "current number of producers = " +this.producers.size()+ "current number of consumers = " +this.consumers.size()+"\n";
   
    return toBeReturned;
}


}


