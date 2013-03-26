package server;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import constantValues.Values;


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
    
	protected class ReplicationInfo {
		
		public int desiredReplication;
		public int currentReplication;
		public int numberOfRemainingAttempts;
	
		public ReplicationInfo(int desired, int current){
			desiredReplication = desired;
			currentReplication = current;
			numberOfRemainingAttempts = Values.number_of_attempts_to_resend_chunks;
		}	
		
		public boolean hasReachedDesiredReplication(){
			return (currentReplication >= desiredReplication);
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