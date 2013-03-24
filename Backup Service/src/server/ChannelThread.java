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
        private final InetAddress src;
        
        public RequestWorker(byte[] request, InetAddress src) {
            this.request = request;
            this.src = src;
        }
        
        @Override
        public void run() {
            processRequest(new String(request),src);
        }
    }

    protected abstract void processRequest(String request,InetAddress src);
    
    public Server getServer(){
		return server;
	}
	public void setServer(Server server){
		this.server = server;
	}
	
}