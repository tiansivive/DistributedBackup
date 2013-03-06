package protocols;




public class Body{
	
	private byte[] chunkData = new byte[64000];

	public Body(byte[] data){
		
		this.chunkData = data;
	}
	
	
	public byte[] getChunkData(){
		return chunkData;
	}
	public void setChunkData(byte[] chunkData){
		this.chunkData = chunkData;
	}

}