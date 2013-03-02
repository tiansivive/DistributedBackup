package server;

import java.io.IOException;
import java.net.MulticastSocket;
import constantValues.Values;



public class BackupChannelThread extends ChannelThread{
	
	private static MulticastSocket multicast_backup_socket;

	public static void init_socket() throws IOException{
		
		multicast_backup_socket = new MulticastSocket(Values.multicast_backup_group_port);
		multicast_backup_socket.joinGroup(Values.multicast_backup_group_address);
	}
	
	public static MulticastSocket getMulticast_backup_socket(){
		return multicast_backup_socket;
	}
	public static void setMulticast_backup_socket(
			MulticastSocket multicast_backup_socket){
		BackupChannelThread.multicast_backup_socket = multicast_backup_socket;
	}
}