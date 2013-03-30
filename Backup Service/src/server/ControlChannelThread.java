package server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Type;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import protocols.Header;
import protocols.ProtocolMessage;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

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
	private HashMap<String, Map<Integer,ReplicationInfo> > ourRequestedBackups;

	/**
	 * The number of replicated chunks of a given file from another machine's backup request that have been stored in other machines.
	 * 
	 */
	private HashMap<String,Map<Integer,Integer>> replicationDegreeOfOthersChunks; //map<ChunkNo,numOfBackups>
	private HashMap<String, Integer> desiredReplicationOfFiles;
	private HashMap<InetAddress,Map<String,ArrayList<Integer>>> storedMessagesReceived;
	private HashMap<String,Set<Integer>> doNotReplyMessages;
	private Set<String> deletedFilesInNetwork;

	//private HashSet<String> completelyBackedUpFiles; 
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


	private ControlChannelThread(Server server){
		setName("ControlThread");
		//completelyBackedUpFiles = new HashSet<String>();
		doNotReplyMessages = new HashMap<String, Set<Integer>>();
		replicationDegreeOfOthersChunks = new HashMap<String, Map<Integer,Integer>>();
		storedMessagesReceived = new HashMap<InetAddress,Map<String,ArrayList<Integer>>>();
		ourRequestedBackups = new HashMap<String,Map<Integer,ReplicationInfo> >();
		desiredReplicationOfFiles = new HashMap<String,Integer>();
		deletedFilesInNetwork = new HashSet<String>();
		setServer(server);
		this.initializeBackgroundMaintenanceProcesses();
	}

	public static ControlChannelThread getInstance(Server server){
		if(instance == null) {
			instance = new ControlChannelThread(server);
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
					this.incomingRequestsPool.execute(new RequestWorker(temp,datagram.getAddress()));
				}
			}catch(IOException e){
				e.printStackTrace();
			}
		}	
	}


	protected void processRequest(String msg, InetAddress src){


		System.out.println("Control Channel - "+Thread.currentThread().getName()+"- Message received:" + msg);
		int endOfHeaderIndex;
		if((endOfHeaderIndex = msg.indexOf("\r\n\r\n")) != -1) { // find the end of the header
			String requestHeader = msg.substring(0, endOfHeaderIndex);
			String headerPattern1 = "^[A-Z]{6,10} (\\d\\.\\d)? [a-z0-9]{64}( [0-9]{1,6})?$";
//			String headerPattern2 = "^GETCHUNK 1.1 [a-z0-9]{64} [0-9]{1,6}$";

			if(requestHeader.matches(headerPattern1)) {
				String[] fields = requestHeader.split(" ");
				Header message = new Header(requestHeader); 

				try {
					switch(fields[0]){
					case "STORED":
					{
						process_StoredMessage(fields,src);
						break;
					}
					case "GETCHUNK":
					{
						process_GetChunkMessage(message,src);
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
					case "DONOTREPLY":
					{
						process_DoNotReplyMessage(message);
						break;
					}
					case "UPDATE":
					{
						process_UpdateMessage(fields,src);
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
	
	private void process_UpdateMessage(String []fields, InetAddress src) {
		if(fields[2].substring(0,12).compareTo("updateupdate") == 0) {
			
			System.out.println("SOMEONE IS ASKING FOR UPDATE");
			
			try {
				// waiting between 0 and 400 miliseconds before decision
				int delay = Server.rand.nextInt(Values.control_thread_update_delay+1);
				Thread.sleep(delay);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
			synchronized (doNotReplyMessages) {
				if(doNotReplyMessages.containsKey(fields[2])) {
					doNotReplyMessages.remove(fields[2]);
					System.out.println("SOMEONE ELSE IS SENDING THE UPDATE!");
					return;
				}
			}
			
			// SEND A DO NOT REPLY MESSAGE TO OTHERS
			String head = new String(Values.UPDATE_DELETED_FILES_MESSAGE + " "
					+ Values.protocol_version + " "
					+ fields[2] + " "
					+ 0);

			byte[] buf = ProtocolMessage.toBytes(head, null);
			DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_control_group_address, Values.multicast_control_group_port);
			try {
				getMulticast_control_socket().send(packet);
			} catch (IOException e1) {
				e1.printStackTrace();
			}

			System.out.println("SENT A DO NOT REPLY MESSAGE TO OTHERS - I AM THE CHOSEN ONE!");
			
			for(String fileId : deletedFilesInNetwork) {
				head = Values.UPDATE_DELETED_FILES_MESSAGE + " "
		                + Values.protocol_version + " "
		                + fileId; 

		        buf = ProtocolMessage.toBytes(head, null);
		        packet = new DatagramPacket(buf, buf.length, src, Values.multicast_control_group_port);
		        
		        try {
					getMulticast_control_socket().send(packet);
					/*
					Thread.sleep(25);
					getMulticast_control_socket().send(packet); // send each update 2x
					*/
					int delay = Server.rand.nextInt(50);
					Thread.sleep(delay);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		} else {
			System.out.println("UPDATE IS BEING MADE. WE EITHER ASKED FOR IT, OR WILL UPDATE OURS ANYWAY");
		}
	}

	private void process_DoNotReplyMessage(Header message){

		HashSet<Integer> tmp = new HashSet<Integer>();
		String fileId = message.getFileID();

		synchronized (doNotReplyMessages) {
			if(!doNotReplyMessages.containsKey(fileId)) {
				tmp.add(message.getChunkNumber());
				doNotReplyMessages.put(fileId, tmp);
			} else {
				this.doNotReplyMessages.get(fileId).add(message.getChunkNumber());
			}
		}
	}

	private void process_GetChunkMessage(Header message, InetAddress srcIP) throws IOException, FileNotFoundException, InterruptedException{

		String fileSeparator = System.getProperty("file.separator");
		File chunk = new File(Values.directory_to_backup_files+fileSeparator+ message.getFileID() +fileSeparator+ "chunk_" + message.getChunkNumber());

		if(chunk.exists()) {

			byte[] chunkData = new byte[64000];
			FileInputStream input = new FileInputStream(chunk);
			int chunkSize = input.read(chunkData);

			if(chunkSize < 64000) {
				byte[] temp = new byte[chunkSize];
				System.arraycopy(chunkData, 0, temp, 0, chunkSize);
				chunkData = temp;
			}

			String head = null;
			byte[] buf = null;
			DatagramPacket packet;

			if( message.getVersion().compareTo("1.0") == 0){
				
				
				head = new String(Values.send_chunk_data_message_identifier + " "
						+ Values.protocol_version + " "
						+  message.getFileID() + " "
						+ message.getChunkNumber());

				buf = ProtocolMessage.toBytes(head, chunkData);
				packet = new DatagramPacket(buf, buf.length, Values.multicast_restore_group_address, Values.multicast_restore_group_port);

				// waiting between 0 and 400 miliseconds before sending response
				int delay = Server.rand.nextInt(Values.restore_channel_send_chunk_delay+1);
				Thread.sleep(delay);

				// CHECK RESTORE THREAD
				if(!getServer().getRestore_thread().hasReceivedChunkMsg(message.getFileID(), message.getChunkNumber())) {
					RestoreChannelThread.getMulticast_restore_socket().send(packet);
					System.out.println(Thread.currentThread().getName() + " sent CHUNK message after processing GETCHUNK message");
				} else {
					System.out.println(getName() + " SOMEBODY BEAT ME TO THE FINISH!");
					getServer().getRestore_thread().clearThisChunkMsg(message.getFileID(), message.getChunkNumber());
				}

			} else {
				// waiting between 0 and 400 miliseconds before decision
				int delay = Server.rand.nextInt(Values.restore_channel_send_chunk_delay+1);
				Thread.sleep(delay);
				
				synchronized (doNotReplyMessages) {
					if(doNotReplyMessages.containsKey(message.getFileID())) {
						Set<Integer> tmp = doNotReplyMessages.get(message.getFileID());
						if(tmp.contains(message.getChunkNumber())) {
							tmp.remove(message.getChunkNumber());
							System.out.println("SOMEONE ELSE IS SENDING THE CHUNK!");
							input.close();
							return;
						}
					}
				}

				// SEND A DO NOT REPLY MESSAGE TO OTHERS
				head = new String(Values.do_not_reply_to_getchunk_message + " "
						+ Values.protocol_version + " "
						+  message.getFileID() + " "
						+ message.getChunkNumber());

				buf = ProtocolMessage.toBytes(head, null);
				packet = new DatagramPacket(buf, buf.length, Values.multicast_control_group_address, Values.multicast_control_group_port);
				getMulticast_control_socket().send(packet);

				System.out.println("SENT A DO NOT REPLY MESSAGE TO OTHERS - I AM THE CHOSEN ONE!");

				// SEND THE CHUNK TO THE REQUESTER
				head = new String(Values.send_chunk_data_message_identifier + " "
						+ Values.protocol_version + " "
						+  message.getFileID() + " "
						+ message.getChunkNumber());

				buf = ProtocolMessage.toBytes(head, chunkData);
				packet = new DatagramPacket(buf, buf.length, srcIP, Values.multicast_restore_group_port);
				RestoreChannelThread.getMulticast_restore_socket().send(packet);
			}
			input.close();
		} 
	}

	private void process_StoredMessage(String[] requestFields, InetAddress src) throws InterruptedException{
		String fileId = requestFields[2];
		int chunkNum = Integer.parseInt(requestFields[3]);
		boolean itMustIncrement = false;
		String debugMessage = "";

		synchronized (storedMessagesReceived) {
			Map<String,ArrayList<Integer>> filesInfo = storedMessagesReceived.get(src);

			if(filesInfo != null) { // already received a store message from this src

				ArrayList<Integer> chunksInfo = filesInfo.get(fileId);
				if(chunksInfo != null) { // already received a store message about this file

					if(!chunksInfo.contains(chunkNum)) { // first stored from this src about this chunk
						chunksInfo.add(chunkNum);
						itMustIncrement = true;
						debugMessage += src.toString()+" RECEIVED FIRST STORED ABOUT NEW CHUNK |"+fileId+":"+chunkNum+"|\n";
					} else {
						debugMessage += src.toString()+" IGNORING RECEIVED STORED |"+fileId+":"+chunkNum+"|\n";
					}
				} else {
					chunksInfo = new ArrayList<Integer>();
					chunksInfo.add(chunkNum);
					filesInfo.put(fileId, chunksInfo);
					itMustIncrement = true;
					debugMessage += src.toString()+" RECEIVED FIRST STORED ABOUT NEW FILE |"+fileId+":"+chunkNum+"|\n";
				}
			} else {
				filesInfo = new HashMap<String,ArrayList<Integer>>();
				ArrayList<Integer> chunksInfo = new ArrayList<Integer>();
				chunksInfo.add(chunkNum);
				filesInfo.put(fileId, chunksInfo);
				storedMessagesReceived.put(src, filesInfo);
				itMustIncrement = true;
				debugMessage += src.toString()+" RECEIVED FIRST STORED FROM PEER |"+fileId+":"+chunkNum+"|\n";
			}
		}

		synchronized (ourRequestedBackups) {
			if(ourRequestedBackups.containsKey(fileId)) { // it's a message about our backups
				if(itMustIncrement) {
					incrementReplicationOfOurChunk(fileId, chunkNum);
					debugMessage += src.toString()+" RECEIVED CONFIRMATION OF OUR BACKUP\n";
				}
			} else { // someone else's backup
				if(itMustIncrement) {
					incrementReplicationOfOtherChunk(fileId, chunkNum);
					synchronized(storedMessagesInformation_Cleaner){
						this.storedMessagesInformation_Cleaner.notifyAll();
					}
					Thread.sleep(20); //wakes up the Cleaner, waits that it changes it's own readyToWork status to true and then changes it to false
					this.storedMessagesInformation_Cleaner.setReadyToWork(false);
					debugMessage += src.toString()+" RECEIVED STORED OF OTHER\n";
				}
			}
		}
		System.out.println(debugMessage);
	}

	private void process_DeleteMessage(Header message){

		String fileSeparator = System.getProperty("file.separator");
		File file = new File(Values.directory_to_backup_files+fileSeparator+ message.getFileID()); 

		if(file.isDirectory() && file.exists()){  
			File[] chunks = file.listFiles();
			for(File f : chunks) {
				f.delete();
			}
			if(file.list().length == 0) {
				file.delete();
			}	
			synchronized(replicationDegreeOfOthersChunks){
				replicationDegreeOfOthersChunks.remove(message.getFileID());
			}
			synchronized(desiredReplicationOfFiles){
				desiredReplicationOfFiles.remove(message.getFileID());
			}
		} else {
			System.out.println("RECEIVED DELETE MSG FOR FILE "+message.getFileID()+" THAT IS NOT BACKED UP IN THIS PEER");
		}
		
		// must save that this file was deleted from the network, even if it wasn't present in this peer
		deletedFilesInNetwork.add(message.getFileID());
		
		synchronized(storedMessagesInformation_Cleaner){
			storedMessagesInformation_Cleaner.notifyAll(); //update information on file
		}
	}

	private void process_RemovedMessage(Header message) throws JsonSyntaxException, JsonIOException, IOException, InterruptedException{

		String fileID = message.getFileID();
		int chunkNum = message.getChunkNumber();

		getServer().addRemovedMessageInfomation(fileID, Integer.toString(chunkNum));
		int delay = Server.rand.nextInt(Values.backup_thread_response_delay)+100; // between 100 and 500 ms
		Thread.sleep(delay);
		System.out.println(Thread.currentThread().getName() + " PROCESSING REMOVED MESSAGE AFTER WAITING " 
															+ delay + " MILISECONDS");
		synchronized(replicationDegreeOfOthersChunks){
			if(replicationDegreeOfOthersChunks.containsKey(fileID)){
				if(replicationDegreeOfOthersChunks.get(fileID).containsKey(chunkNum)){

					int currentReplication = replicationDegreeOfOthersChunks.get(fileID).get(chunkNum);
					currentReplication--;
					replicationDegreeOfOthersChunks.get(fileID).put(chunkNum, currentReplication);				
					System.out.println(Thread.currentThread().getName() 
							+ "\n-------------------------------------------------\n"
							+ "FILE: " + fileID + "\n"
							+ "CHUNK: " + chunkNum + "\n"
							+ "DECREMENTED REPLICATION FROM " + (currentReplication+1) 
							+ " TO " + currentReplication
							+ "\n-------------------------------------------------\n");

					synchronized (storedMessagesInformation_Cleaner) {
						//Activate cleaner to update information. In case replication drops below desired the machine will
						//receive STORED messages, keeping the cleaner dormant until things quiet down 
						storedMessagesInformation_Cleaner.notifyAll();
					}
					if(!hasChunkGotDesiredNumberOfReplicas(fileID, chunkNum)){
						getServer().buildPacketFrom_REMOVED_Message(message, this.desiredReplicationOfFiles.get(fileID));
					}
				
			}else{
				System.out.println(Thread.currentThread().getName() + " CHUNK NOT RECOGNIZED");
			}
		}else{
			System.out.println("FILE NOT RECOGNIZED");
		}
		}
	}


	public void updateRequestedBackups(Header info){

		ReplicationInfo status = new ReplicationInfo(info.getReplicationDegree(), 0);

		synchronized (ourRequestedBackups) {
			Map<Integer,ReplicationInfo> chunksInfo = ourRequestedBackups.get(info.getFileID());
			if(chunksInfo != null) {
				chunksInfo.put(info.getChunkNumber(), status);
			} else {
				chunksInfo = new HashMap<Integer,ReplicationInfo>();
				chunksInfo.put(info.getChunkNumber(), status);
				ourRequestedBackups.put(info.getFileID(), chunksInfo);
			}
		}
	}
	
	public void updateOutRequestedBackupsCurrentReplication(String fileID, int chunkNum){
		
		synchronized (ourRequestedBackups) {
		
			if(ourRequestedBackups.containsKey(fileID)){
				if(ourRequestedBackups.get(fileID).containsKey(chunkNum)){
					
					int currentReplication = ourRequestedBackups.get(fileID).get(chunkNum).currentReplication;
					int updatedReplication = replicationDegreeOfOthersChunks.get(fileID).get(chunkNum);
					ourRequestedBackups.get(fileID).get(chunkNum).currentReplication = updatedReplication;
					
					System.out.println("\n------------------------------------------------\n"
											+ "UPDATING OUR BACKUP REQUEST CURRENT REPLICATION FROM "
											+ currentReplication + " TO " + updatedReplication
											+ "\n------------------------------------------------\n");
				}else{
					System.out.println("CATASTROPHIC FAILURE");
					System.exit(-1);
				}
			}else{
				System.out.println("CATASTROPHIC FAILURE");
				System.exit(-1);
			}
		}
	}

	public void setFilesDesiredReplication(String fileID, int desiredReplication){	
		synchronized (desiredReplicationOfFiles) {
			this.desiredReplicationOfFiles.put(fileID, desiredReplication);
		}
	}

	/**
	 * Increments the number of chunk replicas from someone else's backup request
	 * 
	 * @param file
	 * @param chunkNo
	 */
	public void incrementReplicationOfOtherChunk(String fileId, int chunkNum){

		synchronized (replicationDegreeOfOthersChunks) {

			Map<Integer,Integer> chunksInfo = replicationDegreeOfOthersChunks.get(fileId);

			if(chunksInfo != null) {      
				if(chunksInfo.containsKey(chunkNum)) {
					Integer currentDegree = chunksInfo.get(chunkNum);
					currentDegree += 1;
					chunksInfo.put(chunkNum, currentDegree);
					System.out.println(Thread.currentThread().getName() + " FILE AND CHUNK EXIST - UPDATED REPLICATION FROM " + (currentDegree-1)
							+ " TO " + currentDegree + " |" +fileId+":"+chunkNum+"|");
				} else { 

					chunksInfo.put(chunkNum, 1);//First replica of this particular chunk
					System.out.println(Thread.currentThread().getName() + " FILE EXISTS BUT CHUNK DOES NOT - NEW CHUNK WITH CURRENT REPLICATION 1 |"+fileId+":"+chunkNum+"|");
				}
			}
			else {
				chunksInfo = new HashMap<Integer,Integer>();
				chunksInfo.put(chunkNum, 1);//Same as before, but in case the fileID doesn't exist yet
				replicationDegreeOfOthersChunks.put(fileId, chunksInfo);
				System.out.println("NEW FILE - CHUNK WITH CURRENT REPLICATION 1 |"+fileId+":"+chunkNum+"|");
			}
		}
	}
	/**
	 * Increments a chunk's replicationStatus from one of this machine's backup requests
	 * 
	 * @param file
	 * @param chunkNo
	 */
	public void incrementReplicationOfOurChunk(String fileId, int chunkNum){
		synchronized (ourRequestedBackups) {
			ourRequestedBackups.get(fileId).get(chunkNum).currentReplication++;
			System.out.println("INCREMENTING REPLICATION OF OUR FILE "+fileId+" CHUNK "+chunkNum);
		}
		incrementReplicationOfOtherChunk(fileId, chunkNum);//WE ALSO NEED THIS FOR REMOVED PROTOCOL
	}

	public void notifyDaemonSupervisor() {	
		synchronized(backupRequestsCompletion_Supervisor){
			backupRequestsCompletion_Supervisor.notifyAll();
		}	
	}

	public boolean hasChunkGotDesiredNumberOfReplicas(String fileID, int chunkNum){

		int currentReplication = replicationDegreeOfOthersChunks.get(fileID).get(chunkNum);
		int desiredReplication = desiredReplicationOfFiles.get(fileID);

		return (currentReplication >= desiredReplication);
	}

	public boolean hasChunkGotMoreThanDesiredNumberOfReplicas(String fileID, int chunkNum){

		int currentReplication = replicationDegreeOfOthersChunks.get(fileID).get(chunkNum);
		int desiredReplication = desiredReplicationOfFiles.get(fileID);

		return (currentReplication > desiredReplication);
	}



	private void initializeBackgroundMaintenanceProcesses(){

		backupRequestsCompletion_Supervisor = new Thread(){

			private int delay = 500;
			private HashMap<String,Set<Integer>> chunksWithMissingReplicas;

			public void run(){

				chunksWithMissingReplicas = new HashMap<String, Set<Integer>>();

				try {
					while(true){
						if(ourRequestedBackups.isEmpty()){
							synchronized (getServer()) {
								getServer().notifyAll();
							}
							synchronized (this) {
								System.out.println(this.getName() + " is going to wait...");
								wait();
							}
						} else {
							System.out.println(this.getName() + " REQUESTED BACKUPS IS NOT EMPTY. WE STILL DON'T HAVE ALL FILES WITH OUR DESIRED REPLICATION DEGREE");
							synchronized (this) {
								wait(delay);
							}

							checkCompletionOfBackupRequests();
							if(!chunksWithMissingReplicas.isEmpty()) {
								requestMissingReplicas();
								delay = delay*2;
							} else {
								synchronized(getServer().getControl_thread()){
									ourRequestedBackups.clear();
									chunksWithMissingReplicas.clear();

								}
								delay = 500;
							}
						}
					}			
				}catch (InterruptedException e){
					e.printStackTrace();
				}
			}

			private void requestMissingReplicas() {

				Iterator<String> filesIterator = chunksWithMissingReplicas.keySet().iterator();

				while(filesIterator.hasNext()){

					String fileId = filesIterator.next();

					Iterator<Integer> chunksIterator = chunksWithMissingReplicas.get(fileId).iterator();
					while(chunksIterator.hasNext()){

						int chunkNumber = chunksIterator.next();	
						synchronized (ourRequestedBackups) {

							if(ourRequestedBackups.get(fileId).get(chunkNumber).numberOfRemainingAttempts-- > 0) {
								getServer().send_file(fileId, Integer.toString(chunkNumber));
							} else {
								System.out.println("UNABLE TO REPLICATE CHUNK " + chunkNumber + " OF FILE " + fileId+" WITH DESIRED DEGREE");
								ourRequestedBackups.get(fileId).remove(chunkNumber);
								chunksIterator.remove();
								delay = 500;
							}
						}
					}		
				}		
			}

			private void checkCompletionOfBackupRequests(){

				Iterator<String> filesIterator;

				synchronized(ourRequestedBackups){
					filesIterator = ourRequestedBackups.keySet().iterator();
					while(filesIterator.hasNext()){

						String fileID = filesIterator.next();
						Iterator<Entry<Integer, ReplicationInfo>> chunksIterator = ourRequestedBackups.get(fileID).entrySet().iterator();
						Set<Integer> chunksWithoutDesiredReplication = new HashSet<Integer>();
						boolean hasAtLeastOneReplica = true;

						while(chunksIterator.hasNext()){

							Map.Entry<Integer, ReplicationInfo> pair = chunksIterator.next();
							if(pair.getValue().currentReplication == 0) {
								hasAtLeastOneReplica = false;
							}

							if(!pair.getValue().hasReachedDesiredReplication()){
								chunksWithoutDesiredReplication.add(pair.getKey());
								System.out.println("\n----------------------------------------------------------\n" 
										+ this.getName() + " says in Chunk number "
										+ pair.getKey() + ":\nDesired replication: "
										+ pair.getValue().desiredReplication
										+ "\nCurrent replication: " + pair.getValue().currentReplication
										+ "\nHasn't reached desired replication\n-------------------------------\n");

							}else{
								System.out.println("\n----------------------------------------------------------\n" 
										+ this.getName() + " says in Chunk number "
										+ pair.getKey() + ":\nDesired replication: "
										+ pair.getValue().desiredReplication
										+ "\nCurrent replication: " + pair.getValue().currentReplication
										+ "\nHas reached desired replication\n-------------------------------\n");
								chunksIterator.remove();
							}
						}

						if(hasAtLeastOneReplica) {
							getServer().hasReachedMinimumReplicationDegree(fileID);
						}

						synchronized (chunksWithMissingReplicas) {
							if(chunksWithoutDesiredReplication.isEmpty()){
								//completelyBackedUpFiles.add(fileID);
								filesIterator.remove();
								chunksWithMissingReplicas.remove(fileID);
							}else{
								chunksWithMissingReplicas.put(fileID, chunksWithoutDesiredReplication);
							}
						}
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
						if(replicationDegreeOfOthersChunks.isEmpty()){
							synchronized(this){
								System.out.println(this.getName() + " is waiting");
								this.wait();
							}
						}else{
							synchronized(this){
								System.out.println(this.getName() + " will wait for a minute");
								this.wait(60000); //Wakes up every minute
							}
							System.out.println(this.getName() + " has woken up\n"
									+ "Checkig if clean-up can begin...");



							if(isReadyToWork()){	                    	
								synchronized(this){ //Only one thread can access the file at a time. Wherever else we need to access
									//this file it need to be inside a synchronized(cleanerThread) block

									Gson gson = new Gson();
									
									BufferedReader bufferedReader;
									
									//UPDATE ReplicationDegreeOfOthersChunks FILE
									synchronized(replicationDegreeOfOthersChunks){

										/*
										HashMap<String, Map<Integer,Integer>> toSaveReplicationDegree = null;
										try
										{
											FileInputStream fileIn = new FileInputStream("ReplicationDegreeOfOthersChunks.FAP");
											ObjectInputStream in = new ObjectInputStream(fileIn);
											toSaveReplicationDegree = (HashMap<String,Map<Integer,Integer>>) in.readObject();
											in.close();
											fileIn.close();
										} catch (IOException | ClassNotFoundException e) {
											e.printStackTrace();
										} 
										
										Iterator<String> filesIterator = replicationDegreeOfOthersChunks.keySet().iterator();
										while(filesIterator.hasNext()){ //Update information on file with information on replicationDegreeOfChunks

											String fileID = (String)filesIterator.next();

											if(toSaveReplicationDegree.containsKey(fileID)){

												Iterator<Integer> chunksIterator = replicationDegreeOfOthersChunks.get(fileID).keySet().iterator();
												while(chunksIterator.hasNext()){

													int chunkNumber = chunksIterator.next();
													toSaveReplicationDegree.get(fileID).put(chunkNumber, replicationDegreeOfOthersChunks.get(fileID).get(chunkNumber));	
												}
											}else{
												toSaveReplicationDegree.put(fileID, replicationDegreeOfOthersChunks.get(fileID));
											}
										}
										*/

										try
										{
											FileOutputStream fileOut = new FileOutputStream("ReplicationDegreeOfOthersChunks.FAP");
											ObjectOutputStream out = new ObjectOutputStream(fileOut);
											//out.writeObject(toSaveReplicationDegree);
											out.writeObject(replicationDegreeOfOthersChunks);
											out.close();
											fileOut.close();
										} catch(IOException i) {
											i.printStackTrace();
										}
										System.out.println(Thread.currentThread().getName() + " UPDATED ReplicationDegreeOfOthersChunks FILE");
										//replicationDegreeOfOthersChunks.clear();
									}
									
									//UPDATE desiredReplicationOfFiles FILE
									synchronized(desiredReplicationOfFiles){
										/*
										HashMap<String,Integer> toSaveDesiredReplication = null;
										try
										{
											FileInputStream fileIn = new FileInputStream("DesiredReplicationOfFiles.FAP");
											ObjectInputStream in = new ObjectInputStream(fileIn);
											toSaveDesiredReplication = (HashMap<String,Integer>) in.readObject();
											in.close();
											fileIn.close();
										} catch (IOException | ClassNotFoundException e) {
											e.printStackTrace();
										} 
										
										Iterator<String> filesIterator = desiredReplicationOfFiles.keySet().iterator();
										while(filesIterator.hasNext()){ 
											String fileID = (String)filesIterator.next();
											toSaveDesiredReplication.put(fileID, desiredReplicationOfFiles.get(fileID));
										}
										*/
										
										try
										{
											FileOutputStream fileOut = new FileOutputStream("DesiredReplicationOfFiles.FAP");
											ObjectOutputStream out = new ObjectOutputStream(fileOut);
											//out.writeObject(toSaveDesiredReplication);
											out.writeObject(desiredReplicationOfFiles);
											out.close();
											fileOut.close();
										} catch(IOException i) {
											i.printStackTrace();
										}

										System.out.println(Thread.currentThread().getName() + " UPDATED DesiredReplicationOfFiles FILE");
										//replicationDegreeOfOthersChunks.clear();
									}

									synchronized (storedMessagesReceived) {
										try
										{
											FileOutputStream fileOut = new FileOutputStream("StoredMessagesReceived.FAP");
											ObjectOutputStream out = new ObjectOutputStream(fileOut);
											//out.writeObject(toSaveDesiredReplication);
											out.writeObject(storedMessagesReceived);
											out.close();
											fileOut.close();
										} catch(IOException i) {
											i.printStackTrace();
										}

										System.out.println(Thread.currentThread().getName() + " UPDATED StoredMessagesReceived FILE");
									}
									
									synchronized (deletedFilesInNetwork) {
										try
										{
											FileOutputStream fileOut = new FileOutputStream("DeletedFilesInNetwork.FAP");
											ObjectOutputStream out = new ObjectOutputStream(fileOut);
											//out.writeObject(toSaveDesiredReplication);
											out.writeObject(deletedFilesInNetwork);
											out.close();
											fileOut.close();
										} catch(IOException i) {
											i.printStackTrace();
										}

										System.out.println(Thread.currentThread().getName() + " UPDATED DeletedFilesInNetwork FILE");
									}
									wait();
								}
							}
						}
						this.setReadyToWork(true);              
					}
				} catch (InterruptedException e) {
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

	public static MulticastSocket getMulticast_control_socket(){
		return multicast_control_socket;
	}
	public static void setMulticast_control_socket(
			MulticastSocket multicast_control_socket){
		ControlChannelThread.multicast_control_socket = multicast_control_socket;
	}

	public synchronized Map<Integer, ReplicationInfo> getChunksFromFile(String file){
		return this.ourRequestedBackups.get(file);
	}

	public int getNumberOfBackupsFromChunkNo(String file, int chunkNum){
		synchronized (replicationDegreeOfOthersChunks) {
			try{
				return replicationDegreeOfOthersChunks.get(file).get(chunkNum);
			}catch(NullPointerException e){ 
				return -1;
			}
		}
	}

	public HashMap<String, Map<Integer, Integer>> getReplicationDegreeOfOthersChunks() {
		return replicationDegreeOfOthersChunks;
	}

	public void setReplicationDegreeOfOthersChunks(HashMap<String, Map<Integer, Integer>> replicationDegreeOfOthersChunks) {
		this.replicationDegreeOfOthersChunks = replicationDegreeOfOthersChunks;
	}

	public HashMap<String, Integer> getDesiredReplicationOfFiles() {
		return desiredReplicationOfFiles;
	}

	public void setDesiredReplicationOfFiles(
			HashMap<String, Integer> desiredReplicationOfFiles) {
		this.desiredReplicationOfFiles = desiredReplicationOfFiles;
	}
	
	public HashMap<InetAddress,Map<String,ArrayList<Integer>>> getStoredMessagesReceived() {
		return storedMessagesReceived;
	}
	
	public void setStoredMessagesReceived(HashMap<InetAddress,Map<String,ArrayList<Integer>>> storedMessagesReceived) {
		this.storedMessagesReceived = storedMessagesReceived;
	}
	
	public Set<String> getDeleteFilesInNetwork() {
		return deletedFilesInNetwork;
	}
	
	public void setDeletedFilesInNetwork(Set<String> deleted) {
		deletedFilesInNetwork = deleted;
	}
}
