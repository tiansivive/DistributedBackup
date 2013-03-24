package server;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


// TODO: Auto-generated Javadoc
/**
 * The Class ChannelThread.
 */
public abstract class ChannelThread extends Thread{


    private Server server;
    protected ExecutorService incomingRequestsPool;
    
    protected ChannelThread() {
        incomingRequestsPool = Executors.newCachedThreadPool();
    }

    protected class RequestWorker implements Runnable{

        private final byte[] request;
        private final InetAddress sourceIP;
        
        public RequestWorker(byte[] request) {
            this.request = request;
            this.sourceIP = null;
        }
        
        public RequestWorker(byte[] request, InetAddress ip) {
            this.request = request;
            this.sourceIP = ip;
        }
        
        
        @Override
        public void run() {
        	if(sourceIP == null){
        		processRequest(new String(request));
        	}else{
        		processRequest(new String(request), sourceIP);
        	}
        }
    }

    protected abstract void processRequest(String request);
    protected abstract void processRequest(String request, InetAddress ip);
    
    public Server getServer(){
		return server;
	}
	public void setServer(Server server){
		this.server = server;
	}
	
}