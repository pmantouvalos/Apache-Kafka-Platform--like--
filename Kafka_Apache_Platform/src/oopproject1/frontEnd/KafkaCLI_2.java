package oopproject1.frontEnd;
import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import oopproject1.admin.*;

public class KafkaCLI_2 {

   public static void main(String[] args) {

       KafkaCluster cluster=KafkaCluster.init(); // Δημιουργία νέου KafkaCluster με κατάλληλες παραμέτρους

       Scanner reader = new Scanner(System.in);

       boolean endMenu = false; // Αρχικοποίηση της μεταβλητής πριν το loop
       boolean firstCaseCompleted=false;
       do {

           printMenu();

           int userOption = reader.nextInt();
           reader.nextLine(); // Καθαρισμός της εισόδου

           String givenTopicName, fileName;
           KafkaTopic givenTopic;
           
           switch (userOption) {

               case 1:
            	   try {
            	   System.out.println("You chose to load messages from file...");
                   System.out.println();
                   cluster.listOnlyTopics();
                   System.out.println("Choose one topic from above by writing its name: ");
                   givenTopicName = reader.nextLine();

                   givenTopic = cluster.findTopicByName(givenTopicName);

                   if(cluster.checkTopicExistence(givenTopicName)) {
                       

                       System.out.println("Topic was successfully found ");
                       System.out.println();
                       System.out.println("Write the file name in format: 'something.txt': ");
                       fileName = reader.nextLine();
                       String[] tokens = fileName.split("\\.");
                       if (tokens.length != 2 || !KafkaTopic.checkString(fileName) || !tokens[1].equals("txt")) {
                           System.out.println("Wrong format. Try again");
                           continue;
                       }

                       String filePath = ".\\data\\" + fileName;
                       
                       File file=new File(filePath);
                       
                       if (!file.exists()) {
                           throw new FileNotFoundException("File " + filePath + " does not exist");
                       }

                      
                      
                       
                    	   if(givenTopic.getKeyed()==true) {
                        	   givenTopic.readFirstLine(filePath);
                        	   System.out.println();
                        	   System.out.println("Give key index...(starting from 0)");
                        	   int indexKey;
                        	   indexKey =reader.nextInt();
                        	   if(indexKey >= 0 && indexKey <= Integer.MAX_VALUE) {
                        		   givenTopic.fileReader(filePath, indexKey);
                        	    }
                        	   
                        	   }else if(givenTopic.getKeyed()==false) {
                            
                            		   givenTopic.fileReader(filePath);
                            	    }
                    	   
                    	 
                      
                   } else {
                       throw new IllegalArgumentException("Topic not found. Please try again.");
                   }
                   firstCaseCompleted=true;
                   break;
            	   } catch (Exception e) {
                       System.out.println(e.getMessage());
                       continue;
                   }

               case 2:
            	   try {if(firstCaseCompleted==true) {
                   System.out.println("It seems like you have finished the loading of the messages...");
                   System.out.println();
                   cluster.readAndSaveAll();
           
                  // cluster.saveToFile();
                 //  System.out.println("All the message stacks are stored to files with names broker_host:port_topic_+topicname.txt");
                   System.out.println("Programm terminated.");
                   endMenu = true;
            	   } else {
                       System.out.println("You must select the first option at least once, before starting consuming messages.");
                   }
                   break;
            	   } catch (Exception e) {
                       System.out.println(e.getMessage());
                       continue;
                   }
               case 3:
            	   try {
                   System.out.println("Thanks for using the CLI Menu...");
                   endMenu = true;
                   System.out.println("Programm terminated.");
                   break;
            	   } catch (Exception e) {
                       System.out.println(e.getMessage());
                       continue;
                       
                   }
               default:
                   System.out.println("User option " + userOption + " ignored... Try again..");
                   break;
           }

       } while (!endMenu);
       
   }
   
   public static void printMenu() {
    System.out.println();
    System.out.println("                                 CLI MENU                                        ");
    System.out.println("=================================================================================");
    System.out.println("1. Load Messages from file to topic..............................................");
    System.out.println("2. Press if you are done with loading files to topics............................");
    System.out.println("3. Exit..........................................................................");
    System.out.println("=================================================================================");
   }

}
