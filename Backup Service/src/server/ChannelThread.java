package server;


public class ChannelThread extends Thread{
	
	private Server server;

	
	public Server getServer(){
		return server;
	}

	public void setServer(Server server){
		this.server = server;
	}
	
}