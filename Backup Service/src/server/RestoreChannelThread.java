package server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;

import constantValues.Values;



public class RestoreChannelThread extends ChannelThread{
	
	/**The multicast socket this thread works on
	 * 
	 *  There is only one socket per type of channelThread, i.e the subclasses of ChannelThread;
	 *  That is why it wasn't extracted to the superclass and also why it has to be static.
	 *  */
	private static MulticastSocket multicast_restore_socket;
	private static RestoreChannelThread instance;
	private HashMap<String, byte[]> receivedChunkMessages;
	
	private RestoreChannelThread(){
	    receivedChunkMessages = new HashMap<String,byte[]>();
	}
	
	public static RestoreChannelThread getInstance(){
	    if(instance == null){
	        instance = new RestoreChannelThread();
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
                    incomingRequestsPool.execute(new RequestWorker(temp));
                }
            }catch(IOException e){
                e.printStackTrace();
            } 
        }
	}

	@Override
	protected void processRequest(String request) {
	    int endOfHeaderIndex;
	    if((endOfHeaderIndex = request.indexOf("\r\n\r\n")) != -1) { // find the end of the header
	        String requestHeader = request.substring(0, endOfHeaderIndex);
	        String headerPattern = "^CHUNK 1.0 [a-z0-9]{64} [0-9]{1,6}$";

	        if(requestHeader.matches(headerPattern)) {
	            String[] fields = requestHeader.split(" ");
	            byte[] data = request.substring(endOfHeaderIndex+4).getBytes();

	            System.out.println("RECEIVED CHUNK MESSAGE!! SAVING IT ON THE HASHMAP!");
	            
	            synchronized (this) {
	                receivedChunkMessages.put(fields[2]+":"+fields[3], data);
	            }
	        } else {
	            System.out.println("Invalid header. Ignoring request");
	        }
	    } else {
	        System.out.println("Invalid header. Ignoring request");
	    }
	}

	 public synchronized boolean hasReceivedChunkMsg(String fileId, String chunkNum) {
	     return receivedChunkMessages.containsKey(fileId+":"+chunkNum);
	 }
	 
	 public synchronized void clearThisChunkMsg(String fileId, String chunkNum) {
	     receivedChunkMessages.remove(fileId+":"+chunkNum);
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