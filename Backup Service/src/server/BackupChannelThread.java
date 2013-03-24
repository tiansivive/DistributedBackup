package server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import protocols.ProtocolMessage;
import constantValues.Values;

public class BackupChannelThread extends ChannelThread {
	
	/**The multicast socket this thread works on
	 * 
	 *  There is only one socket per type of channelThread, i.e the subclasses of ChannelThread;
	 *  That is why it wasn't extracted to  the superclass and also why it has to be static.
	 *  */
	private static MulticastSocket multicast_backup_socket;
    private static File backupDirectory;
    private static BackupChannelThread instance;
    
    private HashMap<String,ArrayList<Integer>> backedFiles;
       
	private BackupChannelThread() {
		
		this.setName("BackupChannelThread");
	    backupDirectory = new File(Values.directory_to_backup_files);
	    if(!backupDirectory.mkdir() && !backupDirectory.exists()) {
	        System.out.println("Error creating backups directory. You may not have write permission");
	        System.exit(-1);
	    }
	    backedFiles = new HashMap<String,ArrayList<Integer>>();
	}

	public static BackupChannelThread getInstance() {
	    if(instance == null) {
	        instance = new BackupChannelThread();
	    }
	    return instance;
	}
	
	@Override
    public void run(){
		byte[] buffer = new byte[65000];
		DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
		while(true){
		    try{
		        multicast_backup_socket.receive(datagram);
		        if(!Server.fromThisMachine(datagram.getAddress())){
		            byte[] temp = new byte[datagram.getLength()];
		            System.arraycopy(datagram.getData(), 0, temp, 0, datagram.getLength());
		            incomingRequestsPool.execute(new RequestWorker(temp,datagram.getAddress()));
		        }
		    }catch(IOException e){
		        e.printStackTrace();
		    } 
		}
	}
	
	protected void processRequest(String request, InetAddress src) {

	    int endOfHeaderIndex;
	    if((endOfHeaderIndex = request.indexOf("\r\n\r\n")) != -1) { // find the end of the header
	        String requestHeader = request.substring(0, endOfHeaderIndex);
	        String headerPattern = "^PUTCHUNK 1.0 [a-z0-9]{64} [0-9]{1,6} [1-9]$";
	        System.out.println("\n\n------------------------Received backup request------------------------\n");
	        String[] fields = requestHeader.split(" ");

	        if(requestHeader.matches(headerPattern)) {

	            try {
	                // waiting between 100 and 400 miliseconds before deciding if it will save the chunk
	                int delay = Server.rand.nextInt(301)+100;
	                Thread.sleep(delay);
	            } catch (InterruptedException e1) {
	                e1.printStackTrace();
	            }

	            boolean saveIt = false;
	            
	            synchronized (backedFiles) {
	                ArrayList<Integer> chunksBackedUp = backedFiles.get(fields[2]);
	                if(chunksBackedUp != null) {
	                    if(chunksBackedUp.contains(Integer.parseInt(fields[3]))) { // we have backed it up before
	                        sendStoredMessage(fields);
	                    } else {
	                        saveIt = true;
	                    }
	                } else { // we have no chunk from this file
	                    saveIt = true;
	                }

	                if(saveIt) {
	                    ControlChannelThread cct = getServer().getControl_thread();
	                    if(cct.getNumberOfBackupsFromChunkNo(fields[2], Integer.parseInt(fields[3])) < Integer.parseInt(fields[4])) { // we save it
	                        
	                        sendStoredMessage(fields);
	                        String data = request.substring(endOfHeaderIndex+4);
	                        String fileSeparator = System.getProperty("file.separator");
	                        File directory = new File(Values.directory_to_backup_files+fileSeparator+fields[2]);
	                        File output = new File(Values.directory_to_backup_files+fileSeparator+fields[2]+fileSeparator+"chunk_"+fields[3]);

	                        try {
	                            if(!directory.mkdirs() && !directory.exists()) {
	                                System.out.println("ERROR CREATING FILE DIRECTORY.");
	                            } else {
	                                FileOutputStream fop = new FileOutputStream(output);
	                                fop.write(data.getBytes());
	                                fop.flush();
	                                fop.close();
	                                cct.incrementReplicationOfOtherChunk(fields[2], Integer.parseInt(fields[3]));
	                                
	                                try {
                                        backedFiles.get(fields[2]).add(new Integer(fields[3]));
                                    } catch (NullPointerException e) {
                                        ArrayList<Integer> chunks = new ArrayList<Integer>();
                                        chunks.add(new Integer(fields[3]));
                                        backedFiles.put(fields[2],chunks);
                                    }
	                            }
	                        } catch (IOException e) {
	                            e.printStackTrace();
	                        }
	                    } else {
	                        System.out.println("CHUNK ALREADY HAS DESIRED REPLICATION DEGREE - NO CHUNK BACKUP HERE");
	                    }
	                } else {
	                    System.out.println("WE'VE SAVED THIS CHUNK BEFORE. JUST SENDING STORED MESSAGE.");
	                }
	            } 
	        } else {
	            System.out.println("Invalid header. Ignoring request");
	        }

	    }else{
	        System.out.println("No <CRLF><CRLF> detected. Ignoring request");
	    }
	}
	
	private void sendStoredMessage(String[] fields){
		try{
			String head = new String(Values.stored_chunk_control_message_identifier + " " + fields[1] + " " + fields[2] + " " + fields[3]);
			byte[] buf = ProtocolMessage.toBytes(head, null);
			DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_control_group_address, Values.multicast_control_group_port);
			ControlChannelThread.getMulticast_control_socket().send(packet);
			System.out.println(Thread.currentThread().getName() + " sent STORED message");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Init_socket.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void init_socket() throws IOException {
		
		multicast_backup_socket = new MulticastSocket(Values.multicast_backup_group_port);
		multicast_backup_socket.joinGroup(Values.multicast_backup_group_address);
		multicast_backup_socket.setTimeToLive(1);
	}

	public static MulticastSocket getMulticast_backup_socket() {
		return multicast_backup_socket;
	}
	public static void setMulticast_backup_socket(MulticastSocket multicast_backup_socket) {
		BackupChannelThread.multicast_backup_socket = multicast_backup_socket;
	}
}
