package oopproject1.admin;


/**
*
* @author pmantouvalos,dzoidakis
*/

public class NonKeyedMessage<V> extends Message{
		
	V value;
	
	protected NonKeyedMessage(V value) {
		this.value=value;
        }

	

	@Override
	public void printMessage(){
		System.out.println("Value: "+value);
		
	}


	@Override
    public String toString(){
        return "Value: "+value;

    }
	
	//Getters and Setters with validation (All the expressly mentioned unnecessary setters/getters were removed)
	protected void setValue(V value) {
		this.value = value;
	}
	public V getValue() {
		return value;
	}











}
	
	
