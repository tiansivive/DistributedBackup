package protocols;

import constantValues.Values;


public class Header{
	
	private String messageType;
	private float version;
	private int fileID;
	private int chunkNumber;
	private int replicationDegree;
	
	private byte[] CRLF = new byte[]{Values.header_end_first_byte, Values.header_end_second_byte}; 

	public Header(){
		
		
		
	}
	
	
}