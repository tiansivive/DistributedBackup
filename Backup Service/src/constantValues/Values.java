package constantValues;


import java.net.*;



public class Values{
	
	public static int multicast_control_group_port ;
	public static int multicast_backup_group_port ;
	public static int multicast_restore_group_port ;
	public static InetAddress multicast_control_group_address;
	public static InetAddress multicast_backup_group_address;
	public static InetAddress multicast_restore_group_address;
	
	public static final String backup_chunk_data_message_identifier = "PUTCHUNK";
	public static final String stored_chunk_control_message_identifier = "STORED";
	public static final String recover_chunk_control_message_identifier = "GETCHUNK";
	public static final String send_chunk_data_message_identifier = "CHUNK";
	public static final String file_deleted_control_message_identifier = "DELETE";
	public static final String diskSpace_reclaimed_control_message_identifier = "REMOVED";
	public static final String do_not_reply_to_getchunk_message = "DONOTREPLY";
	
	public static final byte header_end_first_byte = 0xD;
	public static final byte header_end_second_byte = 0xA;
	
	public static String protocol_version = "1.0";
	public static final String directory_to_backup_files = "BACKUPS";
	public static final String directory_to_restore_files = "RESTORED";
	
	public static final int server_sending_packets_delay = 100;
	public static final int backup_thread_response_delay = 400;
	public static final int restore_channel_send_chunk_delay = 400;
	
	public static final int max_number_chunks_sent_simultaneously = 500;
	
	public static final int number_of_attempts_to_resend_chunks = 5;
	public static final int number_of_attempts_to_delete_files = 15;
	
	
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
	public static String getProtocol_version() {
		return protocol_version;
	}
	public static void setProtocol_version(String protocol_version) {
		Values.protocol_version = protocol_version;
	}

	
	
}