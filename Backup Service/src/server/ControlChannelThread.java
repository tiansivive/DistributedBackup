package server;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;


import protocols.Header;
import constantValues.Values;



public class ControlChannelThread extends ChannelThread{
	
	/**The multicast socket this thread works on
	 * 
	 *  There is only one socket per type of channelThread, i.e the subclasses of ChannelThread;
	 *  That is why it wasn't extracted to the superclass and also why it has to be static.
	 *  */
	private static MulticastSocket multicast_control_socket;
	private ExecutorService requestsPool;
	
	private HashMap<String, Map<Integer, ReplicationInfo> > requestedBackups;	
	private HashMap<Integer, Integer> numberOfBackupsPerChunk; //map<ChunkNo,numOfBackups>
	
	private class RequestTask implements Runnable {
		
		private String request;
        public RequestTask(String request) {
            this.request = request;
        }
        
        @Override
        public void run() {
            processRequest(request);
        }
    }
	private class ReplicationInfo{
		
		private int desiredReplication;
		private int currentReplication;
		
		@SuppressWarnings("unused")
		public ReplicationInfo(){
		}
		public ReplicationInfo(int desired, int current){
			this.desiredReplication = desired;
			this.currentReplication = current;
		}

		
		public void incrementCurrentReplication(){
			this.currentReplication++;
		}

		public int getDesiredReplication() {
			return desiredReplication;
		}
		@SuppressWarnings("unused")
		public void setDesiredReplication(int desiredReplication) {
			this.desiredReplication = desiredReplication;
		}
		public int getCurrentReplication() {
			return currentReplication;
		}
		@SuppressWarnings("unused")
		public void setCurrentReplication(int currentReplication) {
			this.currentReplication = currentReplication;
		}
		
	}

	
	
	public ControlChannelThread(){
		
		this.numberOfBackupsPerChunk = new HashMap<Integer, Integer>();
		this.requestedBackups = new HashMap<String, Map<Integer, ReplicationInfo>>();
		this.requestsPool = Executors.newCachedThreadPool();	
	}
	
	
	@Override
	public void run(){
		
		byte[] buffer = new byte[256];
		DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
		
		while(true){
			try{
				multicast_control_socket.receive(datagram);
				//System.out.println(new String(datagram.getData()));
				String msg = new String(datagram.getData()).substring(0, datagram.getLength());
				this.requestsPool.execute(new RequestTask(msg));
			}catch(IOException e){
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	
	}

	private void processRequest(String msg){

		
		System.out.println("\nControl Channel data");
		System.out.println("Message received: " + msg);
	
		Header message = new Header(msg);
		
		switch(message.getMessageType()){
			
			case "STORED":
			{
				process_StoredMessage(message);
				synchronized(this){
					incrementBackupNumberOfChunk(message.getChunkNumber());
				}
				break;
			}
			case "GETCHUNK":
			{
				process_GetChunkMessage(message);
				break;
			}
			case "DELETE":
			{
				process_DeleteMessage(message);
				break;
			}
			case "REMOVED":
			{
				process_RemovedMessage(message);
				break;
			}
			default:
			{
				System.out.println("Unrecognized message type");
				
				//TODO What happens here?!?! probably it's garbage, so maybe discard message?
				break;
			}
		}
	}

	private void process_RemovedMessage(Header message){
		// TODO Auto-generated method stub
		
	}
	private void process_DeleteMessage(Header message){
		// TODO Auto-generated method stub
		
	}
	private void process_GetChunkMessage(Header message){
		// TODO Auto-generated method stub
		
	}
	private void process_StoredMessage(Header message){
		
		if(!this.requestedBackups.containsKey(message.getFileID())){
			
			System.out.println("Received someone else's STORED message\nDiscarding...");
			return;
		}else{
				
			getChunkReplicationInfo(message.getChunkNumber(), message.getFileID()).incrementCurrentReplication();
		}

	}
	
	public synchronized void updateRequestedBackups(Header info){
		
		ReplicationInfo status = new ReplicationInfo(info.getReplicationDegree(), 0);
		Map<Integer, ReplicationInfo> chunkInfo = new HashMap<Integer, ReplicationInfo>();
		chunkInfo.put(info.getChunkNumber(), status);
		
		if(!this.requestedBackups.containsKey(info.getFileID())){
			
			this.requestedBackups.put(info.getFileID(), chunkInfo);					
		}else{
			
			if(!this.getChunksFromFile(info.getFileID()).containsKey(info.getChunkNumber())){
				
				this.getChunksFromFile(info.getFileID()).put(info.getChunkNumber(), status);
			}else{
				//TODO se ja tiver sido feito o pedido de backup deste chunk, acontece o que?
				
				//this.requestedBackups.get(info.getFileID()).get(info.getChunkNumber()).setDesiredReplication(info.getReplicationDegree());
			}
		}
		
	}
	
	
	
	/**
	 * Init_socket.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void init_socket() throws IOException{
		
		multicast_control_socket = new MulticastSocket(Values.multicast_control_group_port);
		multicast_control_socket.joinGroup(Values.multicast_control_group_address);
	}
	
	public void incrementBackupNumberOfChunk(int chunkNo){
		
		int currentNumber = 0;
		
		try{
			currentNumber = this.numberOfBackupsPerChunk.get(chunkNo);
		}catch(NullPointerException e){
		
		}finally{	
			currentNumber++;
			this.numberOfBackupsPerChunk.put(chunkNo, currentNumber);
		}
	}
	
	public ExecutorService getRequestsPool() {
		return requestsPool;
	}
	public void setRequestsPool(ExecutorService requestsPool) {
		this.requestsPool = requestsPool;
	}

	public static MulticastSocket getMulticast_control_socket(){
		return multicast_control_socket;
	}
	public static void setMulticast_control_socket(
			MulticastSocket multicast_control_socket){
		ControlChannelThread.multicast_control_socket = multicast_control_socket;
	}
	
	public synchronized int getNumberOfBackupsFromChunkNo(int chunkNum){
		try{
			return this.numberOfBackupsPerChunk.get(chunkNum);
		}catch(NullPointerException e){
			
			return -1;
		}
	}
	public synchronized HashMap<Integer, Integer> getNumberOfBackupsPerChunk(){
		return numberOfBackupsPerChunk;
	}
	public synchronized void setNumberOfBackupsPerChunk(
			HashMap<Integer, Integer> numberOfBackupsPerChunk) {
		this.numberOfBackupsPerChunk = numberOfBackupsPerChunk;
	}
	public synchronized Map<Integer, ReplicationInfo> getChunksFromFile(String file){
		return this.requestedBackups.get(file);
	}
	public synchronized ReplicationInfo getChunkReplicationInfo(int chunk, String file){	
		return this.requestedBackups.get(file).get(chunk);
	}
	public synchronized int getChunkDesiredReplication(int chunk, String file){		
		return this.requestedBackups.get(file).get(chunk).getDesiredReplication();	
	}
	public synchronized int getChunkCurrentReplicationStatus(int chunk, String file){		
		return this.requestedBackups.get(file).get(chunk).getCurrentReplication();	
	}
	public synchronized HashMap<String, Map<Integer, ReplicationInfo>> getRequestedBackups() {
		return requestedBackups;
	}
	public synchronized void setRequestedBackups(HashMap<String, Map<Integer, ReplicationInfo>> RequestedBackups) {
		this.requestedBackups = RequestedBackups;
	}

}