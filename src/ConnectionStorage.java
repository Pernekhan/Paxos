import java.util.HashMap;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

public class ConnectionStorage {
	public static HashMap <String, ConnectionStorage> storage = new HashMap<String, ConnectionStorage>();
	public Connection connection = null;
	
	public ConnectionStorage(String host)throws Exception{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
	    connection = factory.newConnection();
	}

	public static ConnectionStorage getConnection(String host) throws Exception{
		host = "localhost";
		if (!storage.containsKey(host)){
			storage.put(host, new ConnectionStorage(host));
		}
		return storage.get(host);
	}
	

}
