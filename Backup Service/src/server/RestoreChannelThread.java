package server;

import java.io.IOException;
import java.net.MulticastSocket;
import constantValues.Values;



public class RestoreChannelThread extends ChannelThread{
	
	/**The multicast socket this thread works on
	 * 
	 *  There is only one socket per type of channelThread, i.e the subclasses of ChannelThread;
	 *  That is why it wasn't extracted to the superclass and also why it has to be static.
	 *  */
	private static MulticastSocket multicast_restore_socket;

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