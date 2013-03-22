package server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.MulticastSocket;

import server.BackupChannelThread.RequestWorker;
import constantValues.Values;



public class RestoreChannelThread extends ChannelThread{
	
	/**The multicast socket this thread works on
	 * 
	 *  There is only one socket per type of channelThread, i.e the subclasses of ChannelThread;
	 *  That is why it wasn't extracted to the superclass and also why it has to be static.
	 *  */
	private static MulticastSocket multicast_restore_socket;
	private static RestoreChannelThread instance;
	
	private RestoreChannelThread(){
	    
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
                multicast_backup_socket.receive(datagram);
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

	/**
	 * Init_socket.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void init_socket() throws IOException{
		
		multicast_restore_socket = new MulticastSocket(Values.multicast_restore_group_port);
		multicast_restore_socket.joinGroup(Values.multicast_restore_group_address);
	}
	
	public static MulticastSocket getMulticast_restore_socket(){
		return multicast_restore_socket;
	}
	public static void setMulticast_restore_socket(
			MulticastSocket multicast_restore_socket){
		RestoreChannelThread.multicast_restore_socket = multicast_restore_socket;
	}
}