package com.messagebroker;

import java.util.HashSet;
import java.util.Set;

public class Message {
	
	private String Text;
	private Long time;
	
	

	public Message(String message, long currentTimeMillis) 
	{
		// TODO Auto-generated constructor stub
		this.Text=message;
		this.setTime(currentTimeMillis);
	}

	public String getText() {
		return Text;
	}

	public void setText(String text) {
		Text = text;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	

	
}
