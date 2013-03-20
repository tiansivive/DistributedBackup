package server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import protocols.ProtocolMessage;

import constantValues.Values;

public class BackupChannelThread extends ChannelThread {
	
	/**The multicast socket this thread works on
	 * 
	 *  There is only one socket per type of channelThread, i.e the subclasses of ChannelThread;
	 *  That is why it wasn't extracted to the superclass and also why it has to be static.
	 *  */
	private static MulticastSocket multicast_backup_socket;
    private ExecutorService incomingRequestsPool;
    private final File backupDirectory;
       
	public BackupChannelThread() {
	    incomingRequestsPool = Executors.newCachedThreadPool();
	    backupDirectory = new File(Values.directory_to_backup_files);
	    if(!backupDirectory.mkdir() && !backupDirectory.exists()) {
	        System.out.println("Error creating directory to backup chunks. You may not have write permission");
	        System.exit(-1);
	    }
	}
	
	@Override
    public void run(){
		byte[] buffer = new byte[65000];
		DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
		while(true){
			try{
			    multicast_backup_socket.receive(datagram);
			    final byte[] temp = new byte[datagram.getLength()];
			    System.arraycopy(datagram.getData(), 0, temp, 0, datagram.getLength());
				
			    
                incomingRequestsPool.execute(new RequestWorker(temp));
   
			}catch(IOException e){
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
    }
	
	private void processRequest(String request) {

	    int endOfHeaderIndex;
	    if((endOfHeaderIndex = request.indexOf("\r\n")) != -1) { // find the end of the header

	        String requestHeader = request.substring(0, endOfHeaderIndex);
	        String headerPattern = "^PUTCHUNK 1.0 [a-z0-9]{64} [0-9]{1,6} [0-9]$"; // TODO the replication degree can be 0 ?
	        
	        if(requestHeader.matches(headerPattern)) {
	            String[] fields = requestHeader.split(" ");
	            String data = request.substring(endOfHeaderIndex+4);
	            
	            File output = new File("backup_" + fields[3] + ".txt");
	            
	            try {
		            if(!output.exists()){
		            	
		            	output.createNewFile();
		            }
		            
		            FileOutputStream fop = new FileOutputStream(output);
		            fop.write(data.getBytes());
		            fop.flush();
		            fop.close();
		            
	            } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	            
	            sendStoredMessage(fields);
	        } else {
	            System.out.println("Invalid header. Ignoring request");
	        }
	        
	    } else {
	        System.out.println("No <CRLF><CRLF> detected. Ignoring request");
	    }
	    
	}
	
	public void sendStoredMessage(String[] fields){
		
		try {
			
			String head = new String(Values.stored_chunk_control_message_identifier + " " + fields[1] + " " + fields[2] + " " + fields[3]);
			
			byte[] buf = ProtocolMessage.toBytes(head, null);
			
			DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_control_group_address, Values.multicast_control_group_port);
			ControlChannelThread.getMulticast_control_socket().send(packet);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	private class RequestWorker implements Runnable {
        private final byte[] request;
        public RequestWorker(byte[] request) {
            this.request = request;
        }
        @Override
        public void run() {
            processRequest(new String(request));
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