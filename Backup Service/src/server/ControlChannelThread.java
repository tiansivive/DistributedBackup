package server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
	private static ControlChannelThread instance;
	
	/**
	 * The backup requests this machine sent to others.
	 * Allows this machine to know if the other ones on the network have stored all chunks as well as an adequate number of replicas
	 * 
	 */
	private HashMap<String, Map<Integer, ReplicationInfo> > requestedBackups;
	
	/**
	 * The number of replicated chunks of a given file from another machine's backup request that have been stored in other machines.
	 * 
	 */
	private HashMap<String, Map<Integer, Integer> > numberOfBackupsPerChunk; //map<ChunkNo,numOfBackups>
	
	private HashSet<String> completelyBackedUpFiles; 
	private Thread backupRequestsCompletion_Supervisor;
	
	
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
		public boolean hasReachedDesiredReplication(){		
			return (currentReplication >= desiredReplication);
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

	
	
	private ControlChannelThread(){
		this.setName("ControlThread");
		this.completelyBackedUpFiles = new HashSet<String>();
		this.numberOfBackupsPerChunk = new HashMap<String, Map<Integer, Integer> >();
		this.requestedBackups = new HashMap<String, Map<Integer, ReplicationInfo> >();
		this.requestsPool = Executors.newCachedThreadPool();	
		
		this.initializeBackgroundMaintenanceProcesses();
	}
	
	public static ControlChannelThread getInstance(){
	    if(instance == null) {
	        instance = new ControlChannelThread();
	    }
	    return instance;
	}
	
	
	@Override
	public void run(){
		byte[] buffer = new byte[256];
		DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
				
		while(true){
			try{
			    multicast_control_socket.receive(datagram);
			    if(!Server.fromThisMachine(datagram.getAddress())){
			        String msg = new String(datagram.getData()).substring(0, datagram.getLength());
			        this.requestsPool.execute(new RequestTask(msg));
			    }
			}catch(IOException e){
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	
	}

	private void processRequest(String msg){

		
		System.out.println("\nControl Channel data\nMessage received: " + msg);
	
		Header message = new Header(msg);
		
		switch(message.getMessageType()){
			
			case "STORED":
			{
				process_StoredMessage(message);
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
	private void process_StoredMessage(Header message){
		
		
		System.out.println("cenas");
		if(!this.requestedBackups.containsKey(message.getFileID())){
			
			incrementBackupNumberOfChunk(message.getFileID(), message.getChunkNumber());
			
			System.out.println(Thread.currentThread().getName() + ":\nReceived someone else's STORED message");
			return;
		}else{
			
			this.incrementCurrentReplicationOfChunk(message.getFileID(), message.getChunkNumber());	
			System.out.println(Thread.currentThread().getName() + ":\nReceived confirmation that a replica has been backed up");
		}

	}
	private void process_GetChunkMessage(Header message){
		// TODO Auto-generated method stub
		
	}
	private void process_DeleteMessage(Header message){
		// TODO Auto-generated method stub
		
	}
	private void process_RemovedMessage(Header message){
		// TODO Auto-generated method stub
		
	}
	
	public synchronized void updateRequestedBackups(Header info){
		
		ReplicationInfo status = new ReplicationInfo(info.getReplicationDegree(), 0);
		Map<Integer, ReplicationInfo> chunkInfo = new HashMap<Integer, ReplicationInfo>();
		chunkInfo.put(info.getChunkNumber(), status);
		
		if(!this.requestedBackups.containsKey(info.getFileID())){
			
			this.requestedBackups.put(info.getFileID(), chunkInfo);
			synchronized(this.backupRequestsCompletion_Supervisor){
			
				this.backupRequestsCompletion_Supervisor.notifyAll();
			}
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
	 * Increments the number of chunk replicas from someone else's backup request
	 * 
	 * @param file
	 * @param chunkNo
	 */
	public synchronized void incrementBackupNumberOfChunk(String file, int chunkNo){
		
		int currentNumber = 0;	
		Map<Integer, Integer> tmp = null;
		
		
		if(this.numberOfBackupsPerChunk.containsKey(file)){ //Checks if the file is already in the Map as a key
			if(this.numberOfBackupsPerChunk.get(file).containsKey(chunkNo)){ //Checks if, for this file, this particular chunk already has some replicas
				currentNumber = this.numberOfBackupsPerChunk.get(file).get(chunkNo);
				currentNumber++;
			}else{
				currentNumber = 1;//In this case this particular chunk has never been backed up so this is its first replica
			}
		}else{
			tmp = new HashMap<Integer, Integer>();
			currentNumber = 1; //Here it's the first chunk being backed up for this particular file so, again, it's the first replica
			this.numberOfBackupsPerChunk.put(file, tmp);//Stores this file as a key with the mapped value being empty
		}
		
		this.numberOfBackupsPerChunk.get(file).put(chunkNo, currentNumber);//updates the number of replicas of said chunk
		
	}
	/**
	 * Increments a chunk's replicationStatus from one of this machine's backup requests
	 * 
	 * @param file
	 * @param chunkNo
	 */
	public synchronized void incrementCurrentReplicationOfChunk(String file, int chunkNo){
		
		this.getChunkReplicationInfo(chunkNo, file).incrementCurrentReplication();
	}

	
	
	
	private void initializeBackgroundMaintenanceProcesses(){
		
		backupRequestsCompletion_Supervisor = new Thread(){
			
			private int delay = 500;
			private HashMap<String, Set<Integer>> chunksWithMissigReplicas;
			
			public void run(){
				
				chunksWithMissigReplicas = new HashMap<String, Set<Integer>>();
						
				try {
					while(true){
						
						if(requestedBackups.isEmpty()){
							synchronized(this){
								System.out.println(this.getName() + " is going to wait...");
								this.wait();
							}
						}
						System.out.println(this.getName() + ": aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
						synchronized(this){
							this.wait(delay);
						}
						this.checkCompletionOfBackupRequests();
						
						if(!chunksWithMissigReplicas.isEmpty()){
							requestMissingReplicas();
							delay = delay*2;
						}else{
							
							synchronized(getServer().getControl_thread()){
								requestedBackups.clear();
							}
							delay = 500;
						}
						
						
					}			
				}catch (InterruptedException e){
					
					
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			private void requestMissingReplicas() {
				
				Iterator<?> filesIterator = chunksWithMissigReplicas.keySet().iterator();
				
				while(filesIterator.hasNext()){
					
					String fileID = (String) filesIterator.next();
					
					Iterator<?> chunksIterator = chunksWithMissigReplicas.get(fileID).iterator();
					while(chunksIterator.hasNext()){
						
						String chunkNumber = Integer.toString((Integer) chunksIterator.next());
						getServer().send_file(fileID, chunkNumber);
					}		
				}		
			}

			private void checkCompletionOfBackupRequests(){
				
				Iterator<?> filesIterator;
				
				synchronized(getServer().getControl_thread()){
					filesIterator = requestedBackups.keySet().iterator();
				}
				while(filesIterator.hasNext()){

					String fileID = (String) filesIterator.next();
					Iterator<?> chunksIterator = requestedBackups.get(fileID).entrySet().iterator();
					Set<Integer> chunksWithoutDesiredReplication = new HashSet<Integer>();
					while(chunksIterator.hasNext()){

						@SuppressWarnings("unchecked")
						Map.Entry<Integer, ReplicationInfo> pair = (Entry<Integer, ReplicationInfo>) chunksIterator.next();

						if(!pair.getValue().hasReachedDesiredReplication()){
							chunksWithoutDesiredReplication.add(pair.getKey());

							//TODO re-send PUTCHUNK in Server
						}else{
							//TODO does nothing for now, 
							//CANNOT remove chunks from the requestBackups Hashmap as if a stored message is then received then it'll reset that chunk's currentReplication status to 0
						}
					}

					if(chunksWithoutDesiredReplication.isEmpty()){
						completelyBackedUpFiles.add(fileID);
						synchronized(getServer().getControl_thread()){
							filesIterator.remove();
						}
						chunksWithMissigReplicas.remove(fileID);
					}else{
						chunksWithMissigReplicas.put(fileID, chunksWithoutDesiredReplication);
					}

				}	
			}

		};
		
		backupRequestsCompletion_Supervisor.setName("SupervisorDaemonThread");
		backupRequestsCompletion_Supervisor.setDaemon(true);
		backupRequestsCompletion_Supervisor.start();
				
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
	
	public synchronized int getNumberOfBackupsFromChunkNo(String file, int chunkNum){
		try{
			return this.numberOfBackupsPerChunk.get(file).get(chunkNum);
		}catch(NullPointerException e){	
			return -1;
		}
	}
	public synchronized HashMap<String, Map<Integer, Integer> > getNumberOfBackupsPerChunk(){
		return numberOfBackupsPerChunk;
	}
	public synchronized void setNumberOfBackupsPerChunk(
			HashMap<String, Map<Integer, Integer> > numberOfBackupsPerChunk) {
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