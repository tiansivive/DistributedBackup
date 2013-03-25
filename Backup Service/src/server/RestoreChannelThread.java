package server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import server.ChannelThread.RequestWorker;

import constantValues.Values;



public class RestoreChannelThread extends ChannelThread{
	
	/**The multicast socket this thread works on
	 * 
	 *  There is only one socket per type of channelThread, i.e the subclasses of ChannelThread;
	 *  That is why it wasn't extracted to the superclass and also why it has to be static.
	 *  */
	private static MulticastSocket multicast_restore_socket;
	private static RestoreChannelThread instance;
	private HashMap<String,Set<Integer>> receivedChunkMessages; //Map<FileId,Set<ChunkNumbers>>
	private HashMap<String,RestoreInfo> requestedFileRestorations;
	
	private class RestoreInfo {
	    public String path;
	    public Set<Integer> chunks;
	    public int numberChunks;
	    
	    public RestoreInfo(String path, int numberChunks) {
	        chunks = new HashSet<Integer>();
	        this.path = path;
	        this.numberChunks = numberChunks;
	    }
	}
	
	private RestoreChannelThread(Server server){
	    receivedChunkMessages = new HashMap<String,Set<Integer>>();
	    requestedFileRestorations = new HashMap<String,RestoreInfo>();
	    File restoredDirectory = new File(Values.directory_to_restore_files);
        if(!restoredDirectory.mkdir() && !restoredDirectory.exists()) {
            System.out.println("Error creating restored directory. You may not have write permission");
            System.exit(-1);
        }
	    setServer(server);
	}
	
	public static RestoreChannelThread getInstance(Server server){
	    if(instance == null){
	        instance = new RestoreChannelThread(server);
	    }
	    return instance;
	}
	
	@Override
    public void run(){
	    
        byte[] buffer = new byte[65000];
        DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
        while(true){
            try{
                multicast_restore_socket.receive(datagram);
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
	
	public void addRequestForFileRestoration(String fileId, String path, int numberChunks){
	    synchronized (requestedFileRestorations) {
            if(!requestedFileRestorations.containsKey(fileId)) {
                requestedFileRestorations.put(fileId, new RestoreInfo(path, numberChunks));
            }
        }
	}

	@Override
	protected void processRequest(String request, InetAddress src) {
	    int endOfHeaderIndex;
	    if((endOfHeaderIndex = request.indexOf("\r\n\r\n")) != -1) { // find the end of the header
	        String requestHeader = request.substring(0, endOfHeaderIndex);
	        String headerPattern = "^CHUNK 1.0 [a-z0-9]{64} [0-9]{1,6}$";

	        if(requestHeader.matches(headerPattern)) {
	            String[] fields = requestHeader.split(" ");
	            
	            synchronized (receivedChunkMessages) {
	                try {
	                    receivedChunkMessages.get(fields[2]).add(Integer.parseInt(fields[3]));
	                } catch (NullPointerException e) {
	                    Set<Integer> chunksInfo = new HashSet<Integer>();
	                    chunksInfo.add(Integer.parseInt(fields[3]));
	                    receivedChunkMessages.put(fields[2],chunksInfo);
	                }
                    System.out.println("RECEIVED CHUNK MESSAGE!! SAVING IT ON THE HASHMAP!");
	            }

	            synchronized (requestedFileRestorations) {
	                if(requestedFileRestorations.containsKey(fields[2])) { // we made a request for this file
	                    byte[] data = request.substring(endOfHeaderIndex+4).getBytes();
	                    String fileSeparator = System.getProperty("file.separator");
	                    File directory = new File(Values.directory_to_restore_files+fileSeparator+fields[2]);
	                    File output = new File(Values.directory_to_restore_files+fileSeparator+fields[2]+fileSeparator+"chunk_"+fields[3]);

	                    try {
	                        if(!directory.mkdirs() && !directory.exists()) {
	                            System.out.println("ERROR CREATING RESTORED FILE DIRECTORY.");
	                        } else {
	                            FileOutputStream fop = new FileOutputStream(output);
	                            fop.write(data);
	                            fop.flush();
	                            fop.close();
	                            RestoreInfo info = requestedFileRestorations.get(fields[2]);
	                            info.chunks.add(Integer.parseInt(fields[3]));
	                            
	                            if(info.chunks.size() == info.numberChunks) { // has received all chunks for this file
	                                incomingRequestsPool.execute(new FileFusion(directory,info.path));
	                                requestedFileRestorations.remove(fields[2]);
	                            }
	                        }
	                    } catch (IOException e) {
	                        e.printStackTrace();
	                    }
	                }
	            }
	        } else {
	            System.out.println("Invalid header. Ignoring request");
	        }
	    } else {
	        System.out.println("Invalid header. Ignoring request");
	    }
	}

	public boolean hasReceivedChunkMsg(String fileId, int chunkNum) {
	    synchronized (receivedChunkMessages) {
	        try {
	            return receivedChunkMessages.get(fileId).contains(chunkNum);
	        } catch (NullPointerException e) {
	            return false;
	        }
	    }
	}

	public void clearThisChunkMsg(String fileId, int chunkNum) {
	    synchronized (receivedChunkMessages) {
	        try {
	            receivedChunkMessages.get(fileId).remove(chunkNum);
	        } catch (NullPointerException e) {
	            // do nothing
	        }
        }
	 }

	 /**
	  * Init_socket.
	  *
	  * @throws IOException Signals that an I/O exception has occurred.
	  */
	 public static void init_socket() throws IOException{

	     multicast_restore_socket = new MulticastSocket(Values.multicast_restore_group_port);
	     multicast_restore_socket.joinGroup(Values.multicast_restore_group_address);
	     multicast_restore_socket.setTimeToLive(1);
	 }

	 public static MulticastSocket getMulticast_restore_socket(){
	     return multicast_restore_socket;
	 }
	 public static void setMulticast_restore_socket(
	         MulticastSocket multicast_restore_socket){
	     RestoreChannelThread.multicast_restore_socket = multicast_restore_socket;
	}
}