package oopproject1.admin;

import java.io.Serializable;
import java.util.NoSuchElementException;


public class LinkedStack<E extends Serializable> implements Serializable{

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("hiding")
	private class Node<E> implements Serializable{
		
			
		private static final long serialVersionUID = 1L;
			E key;
	        Node<E> next;

	        Node(E key, Node<E> next) {
	            this.key = key;
	            this.next = next;
	        }
	    }

	    private Node<E> top; // top of stack

	    public boolean isEmpty() {
	        return top == null;
	    }

	    public void push(E obj) {
	        top = new Node<E>(obj, top);
	    }

	    public E pop() {
	        if (isEmpty()) throw new NoSuchElementException();
	        E retval = top.key;
	        top = top.next;
	        return retval;
	    }

	    public E peek() {
	        return isEmpty() ? null : top.key;
	    }
	  
	    //Getters and Setters with validation (All the expressly mentioned unnecessary setters/getters were removed)
		protected Node<E> getTop() {
			return top;
		}

		protected void setTop(Node<E> top) {
			this.top = top;
		}

		protected static long getSerialversionuid() {
			return serialVersionUID;
		}
	   
		
	    
	}
