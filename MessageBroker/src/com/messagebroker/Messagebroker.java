/*
 * Author : Vamshi S.
 * 
 * 
 */
package com.messagebroker;

import javax.inject.Singleton;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.xml.crypto.Data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.ArrayList;
import java.util.Queue;
import java.util.Set;


@Path("/message_broker")
public class Messagebroker 
{
	
	// Data Structure to maintain the publisher and subscribers Information.
	private static Map<String,Set<String>> pdata = new HashMap<String, Set<String>> ();
	
	// Subscriber , Publisher
	private static Map<String,String> sdata = new HashMap<String,String>();

	// Map < Subscriber_name , msg queue specific to subscriber > 
	private static Map<String, Queue<Message> > msg_queue=new HashMap<String, Queue<Message> >();
	
	// subscriber registration times
	private static Map<String,Long> suRegistrationtime = new HashMap<String,Long>();
	// 
	
	
	/*
	 * Publisher Name is Passed as a Path Parameter.
	 *
	 */
	
	@GET
	@Path("/registerPublisher/{publisher_name}")
	@Produces(MediaType.TEXT_PLAIN)
	public String registerPublisher(@PathParam("publisher_name") String publisherName ,@Context HttpHeaders headers)
	{
		Response r = null;
		if(pdata.containsKey(publisherName))
		{
			return "Publisher Already exists.";
		}else
		{
			System.out.println("Publisher Registering : "+publisherName);
			Set<String> a = new HashSet<String>();
			pdata.put(publisherName, a); 
			return "Publisher Registered Succesfully.";
		}
		
	}
	
	/*
	 * method registers the subscribers with a publisher.
	 * subscriber_name and publisher_name are passed in HTTP headers.
	 */
	
	@GET
	@Path("/registerSubscriber")
	@Produces(MediaType.TEXT_PLAIN)
	public String registerSubscriber(@Context HttpHeaders headers)
	{
		
		System.out.println("Subscriber Registering");
		
		String pName=headers.getHeaderString("publisher_name");
		String sName=headers.getHeaderString("subscriber_name");
		
		System.out.println("Subscriber Name - "+sName);
		System.out.println("Publisher Name - "+pName);
		
		if (sdata.containsKey(sName))
		{
			return "Subscriber Already Subscribed";
			
		}else if(pdata.containsKey(pName))
		{
			// Add the publisher information to the subscriber map.
			sdata.put(sName,pName);
			// Add the subscriber information in publisher ds.
			pdata.get(pName).add(sName);
			suRegistrationtime.put(sName,System.currentTimeMillis());
			Queue<Message> q=new LinkedList<Message>();
			msg_queue.put(sName, q);
			
		}else
		{
			return "Publisher doesn't exist";
		}
		
		return "Subscriber Registered Successfully";
	}
	
	/*
	 * This Method is called, when 
	 * 
	 * 
	 */
	
	@POST
	@Path("/sendData")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public Response recveDataFromPublisher(String message,@Context HttpHeaders headers)
	{
		
		String publisher_name=headers.getHeaderString("Publisher_ID");
		
		System.out.println("Receiving Message from the publisher "+publisher_name);
		if(pdata.containsKey(publisher_name))
		{
			Set<String> subscribers = pdata.get(publisher_name);
			/*
			 * current time is really used when the publisher has multiple subscribers
			 */
			Message m=new Message(message,System.currentTimeMillis());
			// Put the message into the message_queue of all the subscribers of publisher.
			for(String subscriber : subscribers)
			{
				msg_queue.get(subscriber).add(m);
			}
		
			/*
			if(DataObjects.msgs.containsKey(publisher_name))
			{
				DataObjects.msgs.get(publisher_name).add(message);
				Set<String> q=DataObjects.msgs.get(publisher_name);
				
				for(String s: q)
				{
					System.out.println(s);
				}
			
			}else
			{
				Set<String> m=new HashSet<String>();
				DataObjects.msgs.put(publisher_name, m);
				DataObjects.msgs.get(publisher_name).add(message);
				Set<String> q=DataObjects.msgs.get(publisher_name);
				for(String s: q)
				{
					System.out.println(s);
				}
			}
			if(global_msg_queue.containsKey(publisher_name))
			{
				
				global_msg_queue.get(publisher_name).add(msg_id);
				Message msg=new Message(message,pdata.get(publisher_name), msg_id); // Shallow , Deep Copy Issue , CONFIRM Once.
				local_msg_queue.put(msg_id, msg);
				
			}else
			{
				
				Set<Integer> s = new HashSet<Integer>();
				global_msg_queue.put(publisher_name, s);
				global_msg_queue.get(publisher_name).add(msg_id);
				Message msg=new Message(message,pdata.get(publisher_name), msg_id); // Shallow , Deep Copy Issue , CONFIRM Once.
				local_msg_queue.put(msg_id, msg);
			
			}
			msg_id++;
			*/
			return Response.ok().entity("Message Received by Broker").build();
		}else
			return Response.ok().entity("Publisher Not Found").build();
	}

	
	
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String testIt()
	{
		return "Hello No_Broker!";
	}	
	
	/*
	 * subscriber invokes this method for messages from specific_publisher.
	 */
	
	
	@GET
	@Path("/receiveData/{publisher_name}")
	@Produces(MediaType.TEXT_PLAIN)
	public String receiveDataFromPublisher(@PathParam("publisher_name") String publisherName)
	{
		return "Data";
	}
	
	
	/*
	 * 
	 * This method sends data to subscriber from all of its publishers.
	 *
	 */
	
	@GET
	@Singleton
	@Path("/recvDataSubs/{subscriber_name}")
	@Produces(MediaType.TEXT_PLAIN)
	public String sendDatatoSubscribers(@PathParam("subscriber_name") String subscriberName)
	{
		// Check whether the subscriber exists
		
		System.out.println(" Received Request from Subscriber for Messages from its publisher "+subscriberName);
		if(sdata.containsKey(subscriberName))
		{
			StringBuilder op=new StringBuilder();
			/*
			 *  Check whether they are any available message's for subscribers.
			 *  If not, send an error message.
			 */
			if(msg_queue.get(subscriberName).isEmpty())
			{
				return "Subscriber has no messages to receive";
			}
			else
			{
				while(!msg_queue.get(subscriberName).isEmpty())
				{
					op.append(msg_queue.get(subscriberName).poll().getText());
				}
				return op.toString();
			}
			
			/*
			// Get a list of all the publisherNames.
			Set<String> publishers = sdata.get(subscriberName);
			for ( String publisher : publishers)
			{
				Set<Integer> msgIds=global_msg_queue.get(publisher);
				for(Integer id : msgIds)
				{
					op.append(local_msg_queue.get(id).getText()+"\n");
					local_msg_queue.get(id).removeSubscriber(subscriberName);
					if(local_msg_queue.get(id).isSubscribersEmpty())
					{
						global_msg_queue.get(publisher).remove(id);
						
					}
				}
			}
			return op.toString();
			*/
		}else
		{
			return "Subscriber Not Found";
		}
	}
		
}
