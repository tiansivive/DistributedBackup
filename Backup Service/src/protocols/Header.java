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
		
		this.messageType = null;
		this.version = -1;
		this.fileID = -1;
		this.chunkNumber = -1;
		this.replicationDegree = -1;
		this.setCRLF(null);
	}
	
	public Header(String info){
		
		String[] args = info.split(" ");
		
		this.messageType = args[0];
		this.version = Integer.parseInt(args[1]);
		this.fileID = Integer.parseInt(args[2]);
		this.chunkNumber = Integer.parseInt(args[3]);
		this.replicationDegree = Integer.parseInt(args[4]);
	}
	
	public Header(String type, float v, int id, int chunkNo, int replicaDegree){
		
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
	public float getVersion(){
		return version;
	}
	public void setVersion(float version){
		this.version = version;
	}
	public int getFileID(){
		return fileID;
	}	
	public void setFileID(int fileID){
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