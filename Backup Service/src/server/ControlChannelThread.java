package server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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
	private HashMap<String, Map<Integer,ReplicationInfo> > ourRequestedBackups;
	
	/**
	 * The number of replicated chunks of a given file from another machine's backup request that have been stored in other machines.
	 * 
	 */
	private HashMap<String,Map<Integer,Integer> > replicationDegreeOfOthersChunks; //map<ChunkNo,numOfBackups>
	private HashMap<InetAddress,Map<String,ArrayList<Integer>>> storedMessagesReceived;
	
	private HashSet<String> completelyBackedUpFiles; 
	private Thread backupRequestsCompletion_Supervisor;
	//private CleanerThread storedMessagesInformation_Cleaner;
	
	/*
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
	*/
	
	private class ReplicationInfo {
		
		public int desiredReplication;
		public int currentReplication;
		public int numberOfRemainingAttempts;
	
		public ReplicationInfo(int desired, int current){
			desiredReplication = desired;
			currentReplication = current;
			numberOfRemainingAttempts = Values.number_of_attempts_to_resend_chunks;
		}	
		
		public boolean hasReachedDesiredReplication(){
			return (currentReplication >= desiredReplication);
		}	
	}

 	private ControlChannelThread(){
		setName("ControlThread");
		completelyBackedUpFiles = new HashSet<String>();
		replicationDegreeOfOthersChunks = new HashMap<String, Map<Integer,Integer>>();
		storedMessagesReceived = new HashMap<InetAddress,Map<String,ArrayList<Integer>>>();
		ourRequestedBackups = new HashMap<String,Map<Integer,ReplicationInfo> >();
		
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
			        this.incomingRequestsPool.execute(new RequestWorker(temp,datagram.getAddress()));
			    }
			}catch(IOException e){
				e.printStackTrace();
			}
		}	
	}

	
	protected void processRequest(String msg, InetAddress src){

        int endOfHeaderIndex;
        if((endOfHeaderIndex = msg.indexOf("\r\n\r\n")) != -1) { // find the end of the header
            String requestHeader = msg.substring(0, endOfHeaderIndex);
            String headerPattern = "^[A-Z]{6,8} (1.0)? [a-z0-9]{64}( [0-9]{1,6})?$";

            if(requestHeader.matches(headerPattern)) {
                String[] fields = requestHeader.split(" ");
                Header message = new Header(requestHeader); // NOT WORKING CORRECTLY

                try {
                    switch(fields[0]){
                    case "STORED":
                    {
                        process_StoredMessage(fields,src);
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
                System.out.println("CONTROL CHANNEL - MESSAGE RECEIVED: "+requestHeader);
                
            } else {
                System.out.println("Unrecognized message type. Ignoring request");
            }
        } else {
            System.out.println("No <CRLF><CRLF> detected. Ignoring request");
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
	                debugMessage += src.toString()+" RECEIVED STORED OF OTHER\n";
	            }
	        }
	    }
	    System.out.println(debugMessage);
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
	    } else {
	        System.out.println("RECEIVED DELETE MSG FOR FILE "+message.getFileID()+" THAT IS NOT BACKED UP IN THIS PEER");
	    }
	}


	private void process_RemovedMessage(Header message){

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
	            Integer currentDegree = chunksInfo.get(chunkNum);
	            if(currentDegree != null) {
	                currentDegree += 1;
	                System.out.println("FILE EXISTS - CHUNK WITH REPLICATION "+currentDegree+" |"+fileId+":"+chunkNum+"|");
	            } else {
	                chunksInfo.put(chunkNum, 1);
	                System.out.println("FILE EXISTS - CHUNK WITH REPLICATION 1 |"+fileId+":"+chunkNum+"|");
	            }
	        }
	        else {
	            chunksInfo = new HashMap<Integer,Integer>();
	            chunksInfo.put(chunkNum, 1);
	            replicationDegreeOfOthersChunks.put(fileId, chunksInfo);
	            System.out.println("NEW FILE - CHUNK WITH REPLICATION 1 |"+fileId+":"+chunkNum+"|");
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
	}
	
	public void notifyDaemonSupervisor() {	
		synchronized(backupRequestsCompletion_Supervisor){
			backupRequestsCompletion_Supervisor.notifyAll();
		}	
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
					
					String fileId = (String) filesIterator.next();
					
					Iterator<Integer> chunksIterator = chunksWithMissingReplicas.get(fileId).iterator();
					while(chunksIterator.hasNext()){
						
						int chunkNumber = (Integer) chunksIterator.next();	
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

				        String fileID = (String) filesIterator.next();
				        Iterator<Entry<Integer, ReplicationInfo>> chunksIterator = ourRequestedBackups.get(fileID).entrySet().iterator();
				        Set<Integer> chunksWithoutDesiredReplication = new HashSet<Integer>();

				        while(chunksIterator.hasNext()){

				            Map.Entry<Integer, ReplicationInfo> pair = (Entry<Integer, ReplicationInfo>) chunksIterator.next();

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
				                //TODO does nothing for now, 
				                //CANNOT remove chunks from the requestBackups Hashmap as if a stored message is then received then it'll reset that chunk's currentReplication status to 0
				            }
				        }

				        synchronized (chunksWithMissingReplicas) {
				            if(chunksWithoutDesiredReplication.isEmpty()){
				                completelyBackedUpFiles.add(fileID);
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
				
		
		/*storedMessagesInformation_Cleaner = new CleanerThread(){
	
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
		*/
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
	            return 0;
	        }
	    }
	}
}
