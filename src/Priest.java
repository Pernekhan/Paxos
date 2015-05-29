import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.StringTokenizer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;

public class Priest {
	int curRound;  // must 
	int promiseRound; // must
	int acks;
	int acceptedRound; 
	ArrayList<String> vc; 
	ArrayList<String> va;  
	ArrayList<String> vd; // must
	int id;
	int N = 3;
	int ids[] = {1, 2, 3};
	
	String request;
	int heartBeatDelay = 1000;
	long lastTimeHeartBeat = 0; 
	long lastTimeCurRound = 0;
	int messageDelay = 1000;
	ArrayList<Integer> Sa;
	ArrayList< ArrayList<String> > Sv;
	ArrayList<Integer> senders;
	boolean decided;
	
	public Priest(int _id)throws Exception{
		id = _id;
		curRound = getCurrentRound();
		vc = new ArrayList<String>();
		promiseRound = getPromiseRound();
		acceptedRound = getAcceptedRound();
		va = getDecidedVector();
		vd = getDecidedVector();
		initThread(id);
		startElectionProcedure();
	}
	

	public void show(){
		System.out.println(vd.toString());
	}
	
	public void process(String req) throws Exception{
		request = req;
		if(curRound == 0) curRound += id;
		else curRound += N;
		setCurRound(curRound);
		vc = new ArrayList<String>();
		acks = 0;
		sendPrepare(curRound);
		checkForDelivery(req, messageDelay, curRound);
	}
	
	void checkForDelivery(final String req, final int delay, final int round){
		Thread thread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					Thread.sleep(delay);
					if (curRound != round) return;
					if (decided) return;
					process(req);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		thread.start();
	}
	

	private void sendPrepare(int curRound) {
		// TODO Auto-generated method stub
		lastTimeCurRound = System.currentTimeMillis();
		decided = false;
		
		Sa = new ArrayList<Integer>();
		Sv = new ArrayList<ArrayList<String>>();
		senders = new ArrayList<Integer>();
		for (int i=0;i<ids.length;i++){
			String message = Utils.initiator + ":" + getId(id) + "|" + Utils.prepare + ":" + curRound;
			sendTo(ids[i], message);
		}
	}

	public String getId(int id){
		return id + ""; //return id + "" + Utils.QUEUE[paxosGroupId];
	}
	public String getId(String id){
		return id + ""; //id + Utils.QUEUE[paxosGroupId];
	}

	public void processMessage(String message)throws Exception{
		if (message.equals("")){ //hearbeat signal
			lastTimeHeartBeat = System.currentTimeMillis();
			return;
		}
		HashMap <String, String> data = parse(message);
		System.out.println("processMessage " + message);
		int toId = Integer.parseInt(data.get(Utils.initiator));
		if (data.get(Utils.prepare) != null){
			int round = Integer.parseInt(data.get(Utils.prepare));
			if (promiseRound < round){
				promiseRound = round;
				setPromiseRound(promiseRound);
				sendPromiseTo(toId, round, acceptedRound, va);
			}
		}
		else if (data.get(Utils.promise) != null){
			int round = Integer.parseInt(data.get(Utils.promise));
			if (curRound != round ){
				System.out.println("Rounds are not equal");
				return;
			}
			long currentTime = System.currentTimeMillis();
			if (currentTime - lastTimeCurRound > 2*messageDelay){
				System.out.println("Message delayed");
				return;
			}
			
			Sa.add(Integer.parseInt(data.get(Utils.acceptedRound)));
			Sv.add(parseArray(data.get(Utils.va)));
			senders.add(toId);
			
			if (Sa.size() == N/2 + 1){
				int v = -1;
				for (int i=0;i<Sa.size();i++){
					if ( v == -1 || Sa.get(i) > Sa.get(v)) v = i;
				}
				
				ArrayList<String> cur = Sv.get(v);
				if (cur.contains(request)){
					System.out.println("You previously added this Command");
					decided = true;
					return;
				}
				cur.add(request);
				vc = cur;
				for (int i=0;i<Sa.size();i++){
					sendAcceptTo(senders.get(i), curRound, vc);
				}
			}
		}
		else if (data.get(Utils.accept) != null){
			int round = Integer.parseInt(data.get(Utils.accept));
			if (round < promiseRound) return;
			promiseRound = round;
			acceptedRound = round;
			va = parseArray(data.get(Utils.va));
			System.out.println("Accepted vector: " + va);
			sendAcceptedTo(toId, round);
		}
		else if (data.get(Utils.accepted) != null){
			int round = Integer.parseInt(data.get(Utils.accepted));
			if (round != curRound) return;
			acks++;
			System.out.println("Accepted num " + acks);
			System.out.println("Initiator id is :" + data.get(Utils.initiator));
			if(acks == N/2 + 1){
				sendDecide(senders, Sa, vc);
			}
		}
		else if (data.get(Utils.decide) != null){
			int round = Integer.parseInt(data.get(Utils.decide));
			ArrayList<String> v = parseArray(data.get(Utils.va));
			System.out.println("Decide " + v.size() + " " + vd.size());
			if (v.size() > vd.size()){
				decided = true;
				setVD(v);
				setAccepted(round);
				show();
			}
		}
		else if (data.get(Utils.request) != null){
			
		}
	}
	
	
	private int getAcceptedRound() throws Exception {
		// TODO Auto-generated method stub
		Scanner in = new Scanner(new File(id+"accepted.in"));
		int res =  in.nextInt();
		in.close();
		return res;
	}

	private int getPromiseRound() throws Exception {
		// TODO Auto-generated method stub
		Scanner in = new Scanner(new File(id+"promise.in"));
		int res =  in.nextInt();
		in.close();
		return res;
	}

	private int getCurrentRound() throws Exception {
		// TODO Auto-generated method stub
		Scanner in = new Scanner(new File(id +"current.in"));
		int res =  in.nextInt();
		in.close();
		return res;
	}

	
	private void setAccepted(int acceptedRound2)throws Exception {
		// TODO Auto-generated method stub
		PrintWriter out = new PrintWriter(new File(id+"accepted.in"));
		out.println(acceptedRound2);
		out.close();
	}

	private void setCurRound(int curRound) throws Exception {
		// TODO Auto-generated method stub		
		PrintWriter out = new PrintWriter(new File(id+"current.in"));
		out.println(curRound);
		out.close();
	}

	private void setPromiseRound(int promiseRound2) throws Exception {
		// TODO Auto-generated method stub
		PrintWriter out = new PrintWriter(new File(id+"promise.in"));
		out.println(promiseRound2);
		out.close();
	}

	private void setVD(ArrayList<String> v) throws Exception {
		// TODO Auto-generated method stub
		vd = v;
		PrintWriter out = new PrintWriter(new File(id+".in"));
		out.println(vd.size());
		for(int i=0;i<vd.size();i++)
			out.print(vd.get(i)+ " ");
		out.close();
	}

	private ArrayList<String> getDecidedVector() throws Exception {
		Scanner in = new Scanner(new File(id+".in"));
		int n = in.nextInt();
		ArrayList<String> res = new ArrayList<String>();
		for (int i=0;i<n;i++){
			res.add(in.next());
		}
		in.close();
		return res;
	}

	private void sendDecide(ArrayList<Integer> senders, ArrayList<Integer> sa, ArrayList<String> vc) {
		// TODO Auto-generated method stub
		String message = Utils.initiator + ":" + getId(this.id) + "|" + Utils.decide + ":" + curRound + "|";
		message += Utils.va + ":";
		for (int i=0;i<vc.size();i++){
			if (i > 0 ) message += ",";
			message += vc.get(i);
		}
		for (int i=0;i<senders.size();i++){
			int toId = senders.get(i);
			sendTo(toId, message);
		}
		
	}

	private void sendAcceptedTo(int toId, int round) {
		// TODO Auto-generated method stub
		String message = Utils.initiator + ":" + getId(this.id) + "|" + Utils.accepted + ":" + round;
		sendTo(toId, message);
	}

	private void sendAcceptTo(int toId, int curRound, ArrayList<String> vc) {
		// TODO Auto-generated method stub
		String message = Utils.initiator + ":" + getId(this.id) + "|" + Utils.accept + ":" + curRound + "|";
		message += Utils.va + ":";
		for (int i=0;i<vc.size();i++){
			if (i > 0) message += ",";
			message += vc.get(i);
		}
		sendTo(toId, message);
	}

	private ArrayList<String> parseArray(String str){
		ArrayList<String> res = new ArrayList<String>();
		if (str == null) return res;
		String a[] = str.split(",");
		for (int i=0;i<a.length;i++)
			res.add(a[i]);
		return res;
	}

	private void sendPromiseTo(int toId, int round, int acceptedRound, ArrayList<String> va) {
		// TODO Auto-generated method stub
		String message = Utils.initiator + ":" + getId(this.id) + "|" + Utils.promise + ":" + round + "|";
		message += Utils.acceptedRound + ":" + acceptedRound + "|";
		message += Utils.va + ":";
		for (int i=0;i<va.size();i++){
			if (i > 0) message += ",";
			message += va.get(i);
		}
		sendTo(toId, message);
	}

	public boolean isLeader() {
		// TODO Auto-generated method stub
		long curTime = System.currentTimeMillis();
		if (curTime - lastTimeHeartBeat > 2*heartBeatDelay) return true;
		return false;
	}
	
	public HashMap<String, String> parse(String message){
		HashMap<String, String> map = new HashMap<String, String>();
		StringTokenizer a = new StringTokenizer(message, "|");
		while(a.hasMoreTokens()){
			String str = a.nextToken();
			String v[] = str.split(":");
			if (v.length == 2){
				map.put(v[0], v[1]);
			}
		}
		return map;
	}
	
	public void initThread(final int _listenId){
		final String listenId = getId(Integer.toString(_listenId));

		Thread thread = new Thread(new Runnable() {

			@Override
			public void run(){
				try {
					Connection connection = ConnectionStorage.getConnection(listenId).connection;
					Channel channel = connection.createChannel();
					channel.queueDeclare(listenId, false, false, false, null);
					QueueingConsumer consumer = new QueueingConsumer(channel);
					channel.basicConsume(listenId, true, consumer);
					while (true) {
						QueueingConsumer.Delivery delivery = consumer.nextDelivery();
						String message = new String(delivery.getBody());
						processMessage(message);
					}
				}catch (Exception e){
					e.printStackTrace();
				}
			}
		});
		thread.start();
	}

	public void sendTo(int toId, String message){
		try {
			if (message.length() > 0)
				System.out.println("[M] " + message + " toId: " + toId);
			Connection connection = ConnectionStorage.getConnection(getId(toId)).connection;
		    Channel channel = connection.createChannel();
		    channel.queueDeclare(getId(toId), false, false, false, null);
		    channel.basicPublish("", getId(toId), null, message.getBytes());
		    channel.close();	
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	void startElectionProcedure(){
		Thread heartBeatThread = new Thread(new Runnable() {
			@Override
			public void run() {
				// TODO Auto-generated method stub
				while(true){
					for (int i=0;i<ids.length;i++){
						if (ids[i] < id){
							sendTo(ids[i], "");
						}
					}
					try {
						Thread.sleep(heartBeatDelay);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}); 
		heartBeatThread.start();
	}

	
	int paxosGroupId = 1;
	
	public void twoPhaseCommit(String request) throws Exception {
		// TODO Auto-generated method stub
		for (int i=0;i<Utils.QUEUE.length;i++){
			sendTwoPhaseRequest("3" + Utils.QUEUE[i], request);
		}
		
	}

	void sendTwoPhaseReqTo(String toId, String message) throws Exception{
		if (message.length() > 0)
			System.out.println("[M] " + message + " toId: " + toId);
		Connection connection = ConnectionStorage.getConnection(getId(toId)).connection;
	    Channel channel = connection.createChannel();
	    channel.queueDeclare(getId(toId), false, false, false, null);
	    channel.basicPublish("", getId(toId), null, message.getBytes());
	    channel.close();			
	}

	private void sendTwoPhaseRequest(String toId, String request) throws Exception {
		// TODO Auto-generated method stub
		String message = Utils.initiator + ":" + getId(id) + "|" + Utils.request + ":" + request;
		sendTwoPhaseReqTo(toId, message);
	}

	
	
}










