package server;

import java.io.IOException;
import java.net.*;

import constantValues.Values;


public class Server{

	private ControlChannelThread control_thread;
	private BackupChannelThread backup_thread;
	private RestoreChannelThread restore_thread;
	
	
	public void mainLoop(){

	}
	
	
	public void run_threads(){
		
		control_thread = new ControlChannelThread();
		backup_thread = new BackupChannelThread();
		restore_thread = new RestoreChannelThread();
		
		control_thread.setServer(this);
		control_thread.start();
		backup_thread.setServer(this);
		backup_thread.start();
		restore_thread.setServer(this);
		restore_thread.start();	
	}
	

 	public static void initialize_sockets() throws IOException{

		ControlChannelThread.init_socket();
		BackupChannelThread.init_socket();
		RestoreChannelThread.init_socket();
		
	}

}