package protocols;


public class ProtocolMessage{
	
	private Header header;
	private Body body;
	
	public ProtocolMessage(){
		
	}
	
	public ProtocolMessage(String head){
		
		String[] args = head.split(" ");
		
		this.header = new Header(args[0],
				args[1], args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]));
		
		this.body = null;
	}
	
	public ProtocolMessage(String head, byte[] data){
		
		String[] args = head.split(" ");
		
		this.header = new Header(args[0],
				args[1], args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]));
		
		this.body = new Body(data);
	}
	
	
	
	public Header getHeader(){
		return header;
	}
	public void setHeader(Header header){
		this.header = header;
	}
	public Body getBody(){
		return body;
	}
	public void setBody(Body body){
		this.body = body;
	}
	
	
	
	
	
}