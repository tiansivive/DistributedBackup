package server;

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
        public RequestWorker(byte[] request) {
            this.request = request;
        }
        @Override
        public void run() {
            processRequest(new String(request));
        }
    }

    protected abstract void processRequest(String request);
    
    public Server getServer(){
		return server;
	}
	public void setServer(Server server){
		this.server = server;
	}
	
}