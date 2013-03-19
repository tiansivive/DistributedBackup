package protocols;

import constantValues.Values;


public class Header{
	
	private String messageType;
	private String version;
	private String fileID;
	private int chunkNumber;
	private int replicationDegree;
	
	private byte[] CRLF = new byte[]{Values.header_end_first_byte, Values.header_end_second_byte}; 

	public Header(){
		
		this.messageType = null;
		this.version = null;
		this.fileID = null;
		this.chunkNumber = -1;
		this.replicationDegree = -1;
		this.setCRLF(null);
	}
	
	public Header(String info){
		
		String[] args = info.split(" ");
		
		this.messageType = args[0];
		this.version = args[1];
		this.fileID = args[2];
		this.chunkNumber = Integer.parseInt(args[3]);
		this.replicationDegree = Integer.parseInt(args[4]);
	}
	
	public Header(String type, String v, String id, int chunkNo, int replicaDegree){
		
		this.messageType = type;
		this.version = v;
		this.fileID = id;
		this.chunkNumber = chunkNo;
		this.replicationDegree = replicaDegree;
	}

	
	public String getMessageType(){
		return messageType;
	}
	public void setMessageType(String messageType){
		this.messageType = messageType;
	}
	public String getVersion(){
		return version;
	}
	public void setVersion(String version){
		this.version = version;
	}
	public String getFileID(){
		return fileID;
	}	
	public void setFileID(String fileID){
		this.fileID = fileID;
	}
	public int getChunkNumber(){
		return chunkNumber;
	}
	public void setChunkNumber(int chunkNumber){
		this.chunkNumber = chunkNumber;
	}	
	public int getReplicationDegree(){
		return replicationDegree;
	}
	public void setReplicationDegree(int replicationDegree){
		this.replicationDegree = replicationDegree;
	}

	public byte[] getCRLF(){
		return this.CRLF;
	}

	public void setCRLF(byte[] cRLF){
		this.CRLF = cRLF;
	}
	
}