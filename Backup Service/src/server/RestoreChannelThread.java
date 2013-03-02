package server;

import java.io.IOException;
import java.net.MulticastSocket;
import constantValues.Values;



public class RestoreChannelThread extends ChannelThread{
	
	private static MulticastSocket multicast_restore_socket;

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