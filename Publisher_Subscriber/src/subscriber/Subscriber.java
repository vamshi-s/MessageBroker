package subscriber;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Scanner;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientResponse;

import java.util.UUID;


public class Subscriber 
{
		public static void main(String[] args) 
		{
			ClientConfig config = new ClientConfig();
			Client client = ClientBuilder.newClient(config);
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			try
			{
				System.out.println("If Subscriber is already registered with a publisher Press 2 else 1 to register subscriber with a publisher");
				int choice =Integer.parseInt(reader.readLine());
			    if (choice == 1 )
				{
			    	System.out.println("Enter Subscriber Name ");
					String subs_name=reader.readLine();
					System.out.println("Enter Publisher Name ");
					String pubs_name=reader.readLine();
					WebTarget target=client.target(getSubscriberRegistrationURI());
					Invocation.Builder invocationBuilder = target.request();
					invocationBuilder.header("publisher_name", pubs_name);
					invocationBuilder.header("subscriber_name", subs_name);
					Response response = invocationBuilder.get();
					System.out.println("Message From Broker REST Service :: ");
					System.out.println(response.readEntity(String.class));
				}
			    System.out.println("Receiving Messages from Publishers");
				System.out.println("Enter Subscriber Name");
				String sName=reader.readLine();
				WebTarget target=client.target(getReceiveMessagesURI(sName));
				while(true)
			    {
			    		Invocation.Builder invocationBuilder = target.request();
						Response resp= invocationBuilder.get();
						System.out.println(resp.readEntity(String.class));
						System.out.println();
						Thread.sleep(10000);
				}	
			}catch(Exception e)
			{
					System.out.println(" ERROR : \n"+ e.getMessage());
			}
		}
		
		private static URI getReceiveMessagesURI(String sName) 
		{
			return UriBuilder.fromUri("http://localhost:8080/MessageBroker/rest/message_broker/recvDataSubs/"+sName).build();
		}


		private static URI getSubscriberRegistrationURI() 
		{
			return UriBuilder.fromUri("http://localhost:8080/MessageBroker/rest/message_broker/registerSubscriber").build();
		}
}
