package server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
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
    private static ExecutorService incomingRequestsPool;
    private static File backupDirectory;
    private static HashMap<String,ArrayList<Integer>> backedFiles;
    private static BackupChannelThread instance;
    private static long numberChunksBackedUp;
    private static ArrayList<InetAddress> machineAddresses;
       
	private BackupChannelThread() {
		
		this.setName("BackupChannelThread");
	    incomingRequestsPool = Executors.newCachedThreadPool();
	    backupDirectory = new File(Values.directory_to_backup_files);
	    if(!backupDirectory.mkdir() && !backupDirectory.exists()) {
	        System.out.println("Error creating backups directory. You may not have write permission");
	        System.exit(-1);
	    }
	    backedFiles = new HashMap<String,ArrayList<Integer>>();
	    numberChunksBackedUp = 0;
	    machineAddresses = new ArrayList<InetAddress>();
	    
	    // not a very elegant solution, but it works
	    Enumeration<NetworkInterface> nets;
	    try {
	        nets = NetworkInterface.getNetworkInterfaces();
	        for (NetworkInterface netint : Collections.list(nets)) {
	            Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
	            for (InetAddress inetAddress : Collections.list(inetAddresses)) {
	                machineAddresses.add(inetAddress);
	            }
	        }
	    } catch (SocketException e) {
	        e.printStackTrace();
	    }
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
			    if(!fromThisMachine(datagram.getAddress())){
			        byte[] temp = new byte[datagram.getLength()];
			        System.arraycopy(datagram.getData(), 0, temp, 0, datagram.getLength());
			        incomingRequestsPool.execute(new RequestWorker(temp));
			    }
			}catch(IOException e){
			    e.printStackTrace();
			} 
		}
	}
	
	private boolean fromThisMachine(InetAddress src){
	    for(InetAddress a : machineAddresses) {
	        if(a.getHostAddress().compareTo(src.getHostAddress()) == 0) {
	            return true;
	        }
	    }
	    return false;
	}

	private void processRequest(String request) {

	    int endOfHeaderIndex;
	    if((endOfHeaderIndex = request.indexOf("\r\n\r\n")) != -1) { // find the end of the header
	        String requestHeader = request.substring(0, endOfHeaderIndex);
	        String headerPattern = "^PUTCHUNK 1.0 [a-z0-9]{64} [0-9]{1,6} [1-9]$";
	        System.out.println("\n\n------------------------Received backup request------------------------\n");
	        String[] fields = requestHeader.split(" ");

	        if(requestHeader.matches(headerPattern)) {
	            if(this.getServer().getControl_thread().getNumberOfBackupsFromChunkNo(fields[2], Integer.parseInt(fields[3])) 
	                    < Integer.parseInt(fields[4])){ //checks if this chunk has a ready been stored the number of desired times

	                String data = request.substring(endOfHeaderIndex+4);
	                File directory = new File(Values.directory_to_backup_files+"/"+fields[2]);
	                File output = new File(Values.directory_to_backup_files+"/"+fields[2]+"/chunk_"+fields[3]);

	                try {
	                    if(!directory.mkdirs() && !directory.exists()) {
	                        System.out.println("Error creating file directory.");
	                    }
	                    if(!output.createNewFile()) {
	                        System.out.println("Chunk already backed up.");
	                        // TODO we have to send another Stored message??? It's sending for now
	                    }
	                    FileOutputStream fop = new FileOutputStream(output);
	                    fop.write(data.getBytes());
	                    fop.flush();
	                    fop.close();
	                    numberChunksBackedUp++;

	                    synchronized (this) { // prevent multiple access to the hashmap
	                        if(backedFiles.containsKey(fields[2])) {
	                            backedFiles.get(fields[2]).add(new Integer(fields[3]));
	                        } else {
	                            backedFiles.put(fields[2], new ArrayList<Integer>());
	                            backedFiles.get(fields[2]).add(new Integer(fields[3]));
	                        }
	                    }

	                } catch (IOException e) {
	                    e.printStackTrace();
	                    // TODO what to do here?
	                }
	                sendStoredMessage(fields);
	            } else {
	                System.out.println("Chunk has already been stored enough times");
	            }
	        } else {

	            System.out.println("Invalid header. Ignoring request");
	        }

	    }else{
	        System.out.println("No <CRLF><CRLF> detected. Ignoring request");
	    }
	}
	
	public void sendStoredMessage(String[] fields){
		try{
			String head = new String(Values.stored_chunk_control_message_identifier + " " + fields[1] + " " + fields[2] + " " + fields[3]);
			byte[] buf = ProtocolMessage.toBytes(head, null);
			DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_control_group_address, Values.multicast_control_group_port);

			// waiting between 0 and 400 miliseconds before sending response
			int delay = Server.rand.nextInt(Values.backup_thread_response_delay+1);
			Thread.sleep(delay);
			ControlChannelThread.getMulticast_control_socket().send(packet);
			System.out.println("Sent STORED message");
			
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private class RequestWorker implements Runnable{
		
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