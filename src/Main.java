import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class Main {
	
	//static int numPriests = 5;
	

	
	public static void main(String args[]) throws Exception{
		Scanner in = new Scanner(System.in);
		//Utils.clearFiles();
		System.out.println("Enter your id: ");
		String str = in.nextLine();
		int id = Integer.parseInt(str);
		Priest p = new Priest(id); 
		
		while(true){
			String request = in.nextLine();
			if (request.equals("-show")){
				p.show();
				continue;
			}
			if (request.charAt(0) == '+' ){
				request = request.substring(1);
				p.twoPhaseCommit(request);
			}
			if (!p.isLeader()){
				System.out.println("I am not a leader");
				continue;
			}
			System.out.println("I am a leader");
			p.process(request);
		}
	}
	
}
