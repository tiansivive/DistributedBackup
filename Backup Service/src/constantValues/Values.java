package constantValues;


import java.net.*;



public class Values{
	
	public static int multicast_control_group_port ;
	public static int multicast_backup_group_port ;
	public static int multicast_restore_group_port ;
	public static InetAddress multicast_control_group_address;
	public static InetAddress multicast_backup_group_address;
	public static InetAddress multicast_restore_group_address;
	
	
	public static int getMulticast_control_group_port() {
		return multicast_control_group_port;
	}
	public static void setMulticast_control_group_port(
			int multicast_control_group_port) {
		Values.multicast_control_group_port = multicast_control_group_port;
	}
	public static void setMulticast_control_group_port(
			String multicast_control_group_port) {
		Values.multicast_control_group_port = Integer.parseInt(multicast_control_group_port);
	}
	
	public static int getMulticast_backup_group_port() {
		return multicast_backup_group_port;
	}
	public static void setMulticast_backup_group_port(
			int multicast_backup_group_port) {
		Values.multicast_backup_group_port = multicast_backup_group_port;
	}
	public static void setMulticast_backup_group_port(
			String multicast_backup_group_port) {
		Values.multicast_backup_group_port = Integer.parseInt(multicast_backup_group_port);
	}	
	
	public static int getMulticast_restore_group_port() {
		return multicast_restore_group_port;
	}
	public static void setMulticast_restore_group_port(
			int multicast_restore_group_port) {
		Values.multicast_restore_group_port = multicast_restore_group_port;
	}
	public static void setMulticast_restore_group_port(
			String multicast_restore_group_port) {
		Values.multicast_restore_group_port = Integer.parseInt(multicast_restore_group_port);
	}
	
	public static InetAddress getMulticast_control_group_address() {
		return multicast_control_group_address;
	}
	public static void setMulticast_control_group_address(
			String multicast_control_group_address) throws UnknownHostException {
		Values.multicast_control_group_address = InetAddress.getByName(multicast_control_group_address);
	}
	
	public static InetAddress getMulticast_backup_group_address() {
		return multicast_backup_group_address;
	}
	public static void setMulticast_backup_group_address(
			String multicast_backup_group_address) throws UnknownHostException {
		Values.multicast_backup_group_address = InetAddress.getByName(multicast_backup_group_address);
	}
	
	public static InetAddress getMulticast_restore_group_address() {
		return multicast_restore_group_address;
	}
	public static void setMulticast_restore_group_address(
			String multicast_restore_group_address) throws UnknownHostException {
		Values.multicast_restore_group_address = InetAddress.getByName(multicast_restore_group_address);
	}

	
	
}