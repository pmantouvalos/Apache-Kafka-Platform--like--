package oopproject1.admin;
/**
*
* @author pmantouvalos,dzoidaki
*/

import java.io.Serializable;

public abstract class Message implements Comparable <Message>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private long ingestionTime;
	
	
	
	
	protected Message() {
    this.ingestionTime= System.currentTimeMillis();
	
	}

	


@Override
public int compareTo(Message nextMessage) {
	
	if(this.ingestionTime>nextMessage.ingestionTime) {
		return 1;
	}
	
	if(this.ingestionTime<nextMessage.ingestionTime) {
		return -1;
	}
	
	return 0;
	
}




protected abstract void printMessage();



//Getters and Setters with validation (All the expressly mentioned unnecessary setters/getters were removed)
protected long getIngestionTime() {
	return ingestionTime;
}



protected void setIngestionTime(long ingestionTime) {
	 if(ingestionTime < 0 || ingestionTime >= Long.MAX_VALUE) {
		 throw new IllegalArgumentException("Ingestion time must represent a valid positive long value");
	 }
	
	
	this.ingestionTime = ingestionTime;
}




}
