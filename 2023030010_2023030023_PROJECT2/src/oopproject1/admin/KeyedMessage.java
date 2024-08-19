package oopproject1.admin;


import java.util.HashMap;

/**
*
* @author pmantouvalos,dzoidakis
*/



public class KeyedMessage<K,V> extends Message{

	K key;
	V value;
	//HashMap<K,V> hmKeyedMessage;
	
	protected KeyedMessage(K key,V value) {
		this.value=value;
		this.key=key;
		//hmKeyedMessage=new HashMap<>();
		//hmKeyedMessage.put(key, value);
		 
        }
	/*private HashMap<K, V> getHmKeyedMessage() {
		return hmKeyedMessage;
	}
	private void setHmKeyedMessage(HashMap<K, V> hmKeyedMessage) {
		this.hmKeyedMessage = hmKeyedMessage;
	}*/


	@Override
	public void printMessage(){
		System.out.println("Key: "+key+" Value: "+value);
		
	}
	
	@Override
    public String toString(){
        return "Key: "+key+" Value: "+value;

    }
	
	//Getters and Setters with validation (All the expressly mentioned unnecessary setters/getters were removed)
	protected K getKey() {
		return key;
	}
	
	protected  K getMessageKey() {
		return this.key;
	}

	protected V getValue() {
		return value;
	}
	protected void setValue(V value) {
		this.value = value;
	}
	protected void setKey(K key) {
		this.key = key;
	}





}
	

