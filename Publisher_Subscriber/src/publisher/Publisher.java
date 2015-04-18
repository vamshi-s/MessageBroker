package publisher;

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

public class Publisher {

	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		
		int choice=0;
		ClientConfig config = new ClientConfig();
		Client client = ClientBuilder.newClient(config);
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		String publisher_name=null;
		System.out.println("If Publisher is Already registered press 2 to send messages else press 1 to Register");
		
		try
		{
			choice =Integer.parseInt(reader.readLine());
			if(choice==1)
			{
				System.out.println("Enter Publisher Name ");
				publisher_name=reader.readLine();
				URI publisher_url = getPublisherURL(publisher_name);
				WebTarget target=client.target(publisher_url);   
				Invocation.Builder invocationBuilder = target.request();
				System.out.println("Registering as Publisher ");
				Response response = invocationBuilder.get();
				System.out.println("Message From Broker REST Service :: ");
				System.out.println(response.readEntity(String.class));
				// If publisher wants to exit with only registering can add some logic here.
			}
			if(publisher_name==null)
			{
				System.out.println("Enter Publisher Name");
				publisher_name=reader.readLine();
			}
			System.out.println(" Enter the Message to Send to Subscribers ");
			WebTarget target=client.target(getSendingURL());
			String text_subsc= reader.readLine();
			Invocation invocation = target.request().header("Publisher_ID",publisher_name).buildPost(Entity.text(text_subsc));
			Response resp= invocation.invoke();
			System.out.println(resp.readEntity(String.class));
			System.out.println();
		}catch(Exception e)
		{
			System.out.println(" ERROR : \n"+ e.getMessage());
		}
	}
	
	private static URI getPublisherURL(String publisher_id) 
	{
		return UriBuilder.fromUri("http://localhost:8080/MessageBroker/rest/message_broker/registerPublisher/"+publisher_id).build();
	}
	
	
	private static URI getSendingURL() 
	{
		return UriBuilder.fromUri("http://localhost:8080/MessageBroker/rest/message_broker/sendData").build();
	}
	
}
