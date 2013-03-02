package server;

import java.io.IOException;
import java.net.MulticastSocket;
import constantValues.Values;



public class ControlChannelThread extends ChannelThread{
	
	
	private static MulticastSocket multicast_control_socket;
	
	
	@Override
	public void run(){
		
	}

	public static void init_socket() throws IOException{
		
		multicast_control_socket = new MulticastSocket(Values.multicast_control_group_port);
		multicast_control_socket.joinGroup(Values.multicast_control_group_address);
	}
	
	/**
	 * @return the multicast_control_socket
	 */

	public static MulticastSocket getMulticast_control_socket(){
		return multicast_control_socket;
	}

	/**
	 * @param multicast_control_socket the multicast_control_socket to set
	 */
	public static void setMulticast_control_socket(
			MulticastSocket multicast_control_socket){
		ControlChannelThread.multicast_control_socket = multicast_control_socket;
	}
	
}