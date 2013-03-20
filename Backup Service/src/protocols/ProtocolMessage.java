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
	
	
	public byte[] toBytes(){
		
		//TODO turn object Header and object Body into bytes.
		
		return null;
	}
	
	public static byte[] toBytes(String head, byte[] body){
		
		int headStringLength = head.getBytes().length;
		int CRLF_length = Header.getCRLF().length;

		
		byte[] headBuf = new byte[headStringLength + CRLF_length*2]; //length of the header information plus length of both <CRLF>
		
		
		System.arraycopy(head.getBytes(), 0, headBuf, 0, headStringLength);
		System.arraycopy(Header.getCRLF(), 0, headBuf, headStringLength, CRLF_length); //First header CRLF
		System.arraycopy(Header.getCRLF(), 0, headBuf, headStringLength + CRLF_length, CRLF_length); //Second header CRLF
		
	
		if(body != null){
			
			byte[] protocolBuf = new byte[headBuf.length + body.length];
			System.arraycopy(headBuf, 0, protocolBuf, 0, headBuf.length);
			System.arraycopy(body, 0, protocolBuf, headBuf.length, body.length);
			return protocolBuf;
		}
		
		return headBuf;
		
			
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