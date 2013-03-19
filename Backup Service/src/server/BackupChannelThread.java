package server;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
	    incomingRequestsPool = Executors.newFixedThreadPool(30);
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
			    byte[] temp = new byte[datagram.getLength()];
			    System.arraycopy(datagram.getData(), 0, temp, 0, datagram.getLength());
                incomingRequestsPool.execute(new RequestWorker(temp));
			}catch(IOException e){
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
    }
	
	private void processRequest(String request) {
	    //System.out.println(request);
	    int endOfHeaderIndex;
	    if((endOfHeaderIndex = request.indexOf("\r\n")) != -1) { // find the end of the header
	        //System.out.println("{: "+request.indexOf("{"));
	        System.out.println("\\n: "+request.indexOf("\n"));
	        System.out.println("\\r: "+request.indexOf("\r"));
	        String requestHeader = request.substring(0, endOfHeaderIndex);
	        String headerPattern = "^PUTCHUNK 1.0 [a-z0-9]{64} [0-9]{1,6} [0-9]$"; // TODO the replication degree can be 0 ?
	        if(requestHeader.matches(headerPattern)) {
	            String[] fields = requestHeader.split(" ");
	            String data = request.substring(endOfHeaderIndex+4);
	        } else {
	            System.out.println("Invalid header. Ignoring request");
	        }
	    } else {
	        System.out.println("No <CRLF><CRLF> detected. Ignoring request");
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