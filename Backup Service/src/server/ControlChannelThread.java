package server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

import protocols.Body;
import protocols.Header;
import protocols.ProtocolMessage;
import constantValues.Values;



public class ControlChannelThread extends ChannelThread{
	
	/**The multicast socket this thread works on
	 * 
	 *  There is only one socket per type of channelThread, i.e the subclasses of ChannelThread;
	 *  That is why it wasn't extracted to the superclass and also why it has to be static.
	 *  */
	private static MulticastSocket multicast_control_socket;
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
	private CleanerThread storedMessagesInformation_Cleaner;
	
	private class CleanerThread extends Thread{
		
		protected boolean readyToWork;
			
		public CleanerThread(){
			this.readyToWork = false;
		}
		public synchronized boolean isReadyToWork(){
			return this.readyToWork;
		}
		public synchronized void setReadyToWork(boolean rtw){
			this.readyToWork = rtw;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
		}
	}
	private class ReplicationInfo{
		
		private int desiredReplication;
		private int currentReplication;
		private int numberOfRemainingAttempts;
	
		
		@SuppressWarnings("unused")
		public ReplicationInfo(){
		}
		public ReplicationInfo(int desired, int current){
			this.desiredReplication = desired;
			this.currentReplication = current;
			this.numberOfRemainingAttempts = Values.number_of_attempts_to_resend_chunks;
		}	
		
		
		public void incrementCurrentReplication(){
			this.currentReplication++;
		}
		public void decrementNumberOfAttempts(){
			this.numberOfRemainingAttempts--;
		}
		public boolean hasReachedDesiredReplication(){
			return (currentReplication >= desiredReplication);
		}	
		public boolean canResendChunk(){
			return (this.numberOfRemainingAttempts > 0);
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
		public void setCurrentReplication(int currentReplication) {
			this.currentReplication = currentReplication;
		}
		@SuppressWarnings("unused")
		public int getNumberOfRemainingAttempts() {
			return numberOfRemainingAttempts;
		}
		@SuppressWarnings("unused")
		public void setNumberOfRemainingAttempts(int numberOfRemainingAttempts) {
			this.numberOfRemainingAttempts = numberOfRemainingAttempts;
		}
		
		
	}

 	private ControlChannelThread(){
		this.setName("ControlThread");
		this.completelyBackedUpFiles = new HashSet<String>();
		this.numberOfBackupsPerChunk = new HashMap<String, Map<Integer, Integer> >();
		this.requestedBackups = new HashMap<String, Map<Integer, ReplicationInfo> >();
		
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
			        byte[] temp = new byte[datagram.getLength()];
                    System.arraycopy(datagram.getData(), 0, temp, 0, datagram.getLength());
			        this.incomingRequestsPool.execute(new RequestWorker(temp));
			    }
			}catch(IOException e){
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	
	}

	
	protected void processRequest(String msg){

	    System.out.println("Control Channel - Message received: " + msg);
        int endOfHeaderIndex;

        if((endOfHeaderIndex = msg.indexOf("\r\n\r\n")) != -1) { // find the end of the header
            String requestHeader = msg.substring(0, endOfHeaderIndex);
            String headerPattern = "^[A-Z]{6,8} (1.0)? [a-z0-9]{64} ([0-9]{1,6})?$";

            if(requestHeader.matches(headerPattern)) {
                Header message = new Header(msg);

                try {
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
                        System.out.println("Unrecognized message type. Ignoring request");
                        break;
                    }
                    }
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("Unrecognized message type. Ignoring request");
            }
        } else {
            System.out.println("No <CRLF><CRLF> detected. Ignoring request");
        }
	}
	
	private void process_StoredMessage(Header message) throws InterruptedException{
		
		synchronized(storedMessagesInformation_Cleaner){
			this.storedMessagesInformation_Cleaner.notifyAll();
		}
		Thread.sleep(50); //wakes up the Cleaner, waits that it changes it's own readyToWork status to true and then changes it to false
		this.storedMessagesInformation_Cleaner.setReadyToWork(false);
		if(!this.requestedBackups.containsKey(message.getFileID())){
			incrementBackupNumberOfChunk(message.getFileID(), message.getChunkNumber());
			System.out.println(Thread.currentThread().getName() + " received someone else's STORED message");
		}else{
			this.incrementCurrentReplicationOfChunk(message.getFileID(), message.getChunkNumber());	
			System.out.println(Thread.currentThread().getName() + " received confirmation that a replica has been backed up");
		}
	}

	
	private void process_GetChunkMessage(Header message) throws IOException, FileNotFoundException, InterruptedException{
		
	    File chunk = new File(Values.directory_to_backup_files+ "/" + message.getFileID() + "/chunk_" + message.getChunkNumber());

	    if(chunk.exists()) {

	        byte[] chunkData = new byte[64000];
	        FileInputStream input = new FileInputStream(chunk);

	        int chunkSize = input.read(chunkData);

	        if(chunkSize < 64000) {
	            byte[] temp = new byte[chunkSize];
	            System.arraycopy(chunkData, 0, temp, 0, chunkSize);
	            chunkData = temp;
	        }

	        String head = new String(Values.send_chunk_data_message_identifier + " "
	                + Values.protocol_version + " "
	                +  message.getFileID() + " "
	                + message.getChunkNumber());


	        byte[] buf = ProtocolMessage.toBytes(head, chunkData);
	        DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_restore_group_address, Values.multicast_restore_group_port);

	        // waiting between 0 and 400 miliseconds before sending response
	        int delay = Server.rand.nextInt(Values.restore_channel_send_chunk_delay+1);
	        Thread.sleep(delay);

	        // CHECK RESTORE THREAD
	        if(!getServer().getRestore_thread().hasReceivedChunkMsg(message.getFileID(), Integer.toString(message.getChunkNumber()))) {
	            RestoreChannelThread.getMulticast_restore_socket().send(packet);
	            System.out.println(Thread.currentThread().getName() + " sent CHUNK message after processing GETCHUNK message");
	        } else {
	            System.out.println(getName() + " SOMEBODY BEAT ME TO THE FINISH!");
	            getServer().getRestore_thread().clearThisChunkMsg(message.getFileID(), Integer.toString(message.getChunkNumber()));
	        }
	    } else {
	        // TODO TELL RESTORE THREAD TO IGNORE CHUNKS MESSAGES FOR THIS FILE ID AND CHUNK NUMBER
	    }

	}


	private void process_DeleteMessage(Header message){
		
		 File file = new File(Values.directory_to_backup_files + "/" + message.getFileID());      	 
		 String[] chunks;      

		 if(file.isDirectory()){  
			 chunks = file.list();  
			 for (int i = 0; i < chunks.length; i++) {  
				 File myFile = new File(file, chunks[i]);
				 myFile.delete();
			 }  
		 }else{
			 System.out.println("Received DELETE message but no files were found that matched the specified file.");
			 //TODO
		 }
	}
	
	
	private void process_RemovedMessage(Header message){
		
	}
	
	
	public synchronized void updateRequestedBackups(Header info){
		
		ReplicationInfo status = new ReplicationInfo(info.getReplicationDegree(), 0);
		Map<Integer, ReplicationInfo> chunkInfo = new HashMap<Integer, ReplicationInfo>();
		chunkInfo.put(info.getChunkNumber(), status);
		
		if(!this.requestedBackups.containsKey(info.getFileID())){
			
			this.requestedBackups.put(info.getFileID(), chunkInfo);
			/*synchronized(this.backupRequestsCompletion_Supervisor){		
				this.backupRequestsCompletion_Supervisor.notifyAll();
			}*/
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
				System.out.println("\n------------------------------\n"
									+"INCREMENTING BACKUP REPLICAS OF CHUNK " + chunkNo + " TO: " + currentNumber
									+"\n------------------------------\n");
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
	/**
	 * 
	 * Resets a chunk's number of replicas to 0, after receiving a PUTCHUNK, in order to properly count new in-bound STORED messages
	 * 
	 * @param file
	 * @param chunkNo
	 */
	public synchronized boolean resetChunkReplicationStatus(String file, String chunkNo){
			
		int currentNumber = 0; //Resetting replicas to 0
		Map<Integer, Integer> tmp = null;
			
		if(!this.numberOfBackupsPerChunk.containsKey(file)){ //Checks if the file is already in the Map as a key	
			
			tmp = new HashMap<Integer, Integer>();
			this.numberOfBackupsPerChunk.put(file, tmp);//Stores this file as a key with the mapped value being empty
		}else{
			System.out.println("FILE EXISTS, CHECKING IF CHUNK EXISTS");
			if(this.numberOfBackupsPerChunk.get(file).containsKey(Integer.parseInt(chunkNo))){
				
				System.out.println("CHUNK EXISTS");
				if(this.numberOfBackupsPerChunk.get(file).get(Integer.parseInt(chunkNo)) == 0){
					System.out.println("REPLICATION ALREADY 0, WAITING.....");
					return false;
				}	
			}		
		}
		System.out.println("RESETTING FROM " + this.numberOfBackupsPerChunk.get(file).get(Integer.parseInt(chunkNo)) + " TO 0");
		this.numberOfBackupsPerChunk.get(file).put(Integer.parseInt(chunkNo), currentNumber);//updates the number of replicas of said chunk
		return true;
	}
	
	public void notifyDaemonSupervisor() {	
		synchronized(backupRequestsCompletion_Supervisor){
			backupRequestsCompletion_Supervisor.notifyAll();
		}	
	}

	
	private void initializeBackgroundMaintenanceProcesses(){
		
		backupRequestsCompletion_Supervisor = new Thread(){
			 
			private int delay = 500;
			private HashMap<String, Set<Integer>> chunksWithMissingReplicas;
			
			public void run(){
				
				chunksWithMissingReplicas = new HashMap<String, Set<Integer>>();
						
				try {
					while(true){
						
						if(requestedBackups.isEmpty()){
							synchronized(this){
								System.out.println(this.getName() + " is going to wait...");
								this.wait();
								Thread.sleep(5000);// Wait 5 seconds to start re-sending chunks
								System.out.println(this.getName() + " has waken up...");
								continue;
							}
						}
						System.out.println(this.getName() + " REQUESTED BACKUPS IS NOT EMPTY. WE STILL DON'T HAVE ALL FILES WITH OUR DESIRED REPLICATION DEGREE");
						synchronized(this){
							this.wait(delay);
						}
						this.checkCompletionOfBackupRequests();
						
						if(!chunksWithMissingReplicas.isEmpty()){
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
				
				Iterator<String> filesIterator = chunksWithMissingReplicas.keySet().iterator();
				
				while(filesIterator.hasNext()){
					
					String fileID = (String) filesIterator.next();
					
					Iterator<Integer> chunksIterator = chunksWithMissingReplicas.get(fileID).iterator();
					while(chunksIterator.hasNext()){
						
						int chunkNumber = (Integer) chunksIterator.next();			
						synchronized(getServer().getControl_thread()){
							//Sending chunk again, going to count number of STORED messages again
							getChunkReplicationInfo(chunkNumber, fileID).setCurrentReplication(0);
						}
						if(getChunkReplicationInfo(chunkNumber, fileID).canResendChunk()){
							getServer().send_file(fileID, Integer.toString(chunkNumber));
							getChunkReplicationInfo(chunkNumber, fileID).decrementNumberOfAttempts();
						}else{
							
							System.out.println("Unable to fully backup chunk " + chunkNumber + " of " + fileID);
							requestedBackups.get(fileID).remove(chunkNumber);
							chunksIterator.remove();
							delay = 500; //If a chunk hasn't been fully backed up and requestedBackups isn't empty then a second Backup request has been made, so we reset the delay
						}
					}		
				}		
			}

			private void checkCompletionOfBackupRequests(){
				
				Iterator<String> filesIterator;
				
				synchronized(getServer().getControl_thread()){
					filesIterator = requestedBackups.keySet().iterator();
				}
				while(filesIterator.hasNext()){

					String fileID = (String) filesIterator.next();
					Iterator<Entry<Integer, ReplicationInfo>> chunksIterator = requestedBackups.get(fileID).entrySet().iterator();
					Set<Integer> chunksWithoutDesiredReplication = new HashSet<Integer>();
					while(chunksIterator.hasNext()){

						Map.Entry<Integer, ReplicationInfo> pair = (Entry<Integer, ReplicationInfo>) chunksIterator.next();
						
						if(!pair.getValue().hasReachedDesiredReplication()){
							chunksWithoutDesiredReplication.add(pair.getKey());
							System.out.println("\n----------------------------------------------------------\n" 
												+ this.getName() + " says in Chunk number "
												+ pair.getKey() + ":\nDesired replication: "
												+ pair.getValue().getDesiredReplication()
												+ "\nCurrent replication: " + pair.getValue().getCurrentReplication()
												+ "\nHasn't reached desired replication\n-------------------------------\n");
							
						}else{
							System.out.println("\n----------------------------------------------------------\n" 
									+ this.getName() + " says in Chunk number "
									+ pair.getKey() + ":\nDesired replication: "
									+ pair.getValue().getDesiredReplication()
									+ "\nCurrent replication: " + pair.getValue().getCurrentReplication()
									+ "\nHas reached desired replication\n-------------------------------\n");
							//TODO does nothing for now, 
							//CANNOT remove chunks from the requestBackups Hashmap as if a stored message is then received then it'll reset that chunk's currentReplication status to 0
						}
					}

					if(chunksWithoutDesiredReplication.isEmpty()){
						completelyBackedUpFiles.add(fileID);
						synchronized(getServer().getControl_thread()){
							filesIterator.remove();
						}
						chunksWithMissingReplicas.remove(fileID);
					}else{
						chunksWithMissingReplicas.put(fileID, chunksWithoutDesiredReplication);
					}

				}	
			}

		};
		
		backupRequestsCompletion_Supervisor.setName("SupervisorDaemonThread");
		backupRequestsCompletion_Supervisor.setDaemon(true);
		backupRequestsCompletion_Supervisor.start();
				
		
		storedMessagesInformation_Cleaner = new CleanerThread(){
	
			public void run(){
				
				try {
					while(true){
		
						if(numberOfBackupsPerChunk.isEmpty()){
							synchronized(this){
								System.out.println(this.getName() + " is waiting");
								this.wait();
							}
						}else{
							synchronized(this){
								this.wait(60000); //Wakes up every minute if 
							}
							System.out.println(this.getName() + " is woke up\n"
									  						  + "Checkig if clean-up can begin...");
							if(isReadyToWork()){
								
								numberOfBackupsPerChunk.clear();
								//TODO anything else needing cleanup?
							}
						}
						this.setReadyToWork(true);		
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		};
		
		storedMessagesInformation_Cleaner.setName("CleanerDaemonThread");
		storedMessagesInformation_Cleaner.setDaemon(true);
		storedMessagesInformation_Cleaner.start();
	}
	
	
	
	/**
	 * Init_socket.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void init_socket() throws IOException{
		
		multicast_control_socket = new MulticastSocket(Values.multicast_control_group_port);
		multicast_control_socket.joinGroup(Values.multicast_control_group_address);
		multicast_control_socket.setTimeToLive(1);
	}
	
	public ExecutorService getRequestsPool() {
		return incomingRequestsPool;
	}
	public void setRequestsPool(ExecutorService requestsPool) {
		this.incomingRequestsPool = requestsPool;
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