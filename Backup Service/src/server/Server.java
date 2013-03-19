package server;

import java.io.*;
import java.net.*;
import java.util.*;
import protocols.*;
import com.google.gson.*;

import constantValues.Values;


public class Server{

	private ControlChannelThread control_thread;
	private BackupChannelThread backup_thread;
	private RestoreChannelThread restore_thread;

	public class FileToBackup {
		public String path;
		public int replicationDegree;
	}

	public class Config {
		public int availableSpaceOnServer;
		public ArrayList<FileToBackup> filesToBackup;
		public boolean serverIsActive;
	}

	public void mainLoop() {
		run_threads();

		/*Gson gson = new Gson();
		BufferedReader bufferedReader = null;

		try {
			System.out.println("Opening config file");
			bufferedReader = new BufferedReader(new FileReader("config.json"));
		} catch (FileNotFoundException e) {
			//e.printStackTrace();
			System.out.println("Configuration file is missing. Shutting down the server.");
			System.exit(-1);
		}

		Config config = gson.fromJson(bufferedReader, Config.class);

		// go through each file and send it
		for(FileToBackup f : config.filesToBackup) {
			File file = new File(f.path);
			if(file.exists()) {
				if(file.isDirectory()) {
					send_directory(file, f.replicationDegree);
				} else {
					send_file(file, f.replicationDegree);

				}
			} else {
				System.out.println("The file/dir "+file.getName()+" doesn't exist!");
			}
		}*/
	}

	public void send_directory(File directory, int replicationDegree) {
		File[] files = directory.listFiles();
		for(File f : files) {
			if(f.isDirectory()) {
				send_directory(f, replicationDegree);
			} else {
				if(!f.isHidden()) {
					send_file(f, replicationDegree);
				}
			}
		}
	}

	public void send_file(final File file, final int replicationDegree) {


		String fileIdentifier;
		System.out.println("Sending file "+file.getName()+" with replication degree: "+replicationDegree+" size: "+file.length());
		System.out.println("File identifier: "+(fileIdentifier = HashString.getFileIdentifier(file)));
		try {
			FileInputStream fileInputStream = new FileInputStream(file);
			byte[] dataBytes = new byte[64000];
			int nread = -1;
			int chunkNum = 0;
			while ((nread = fileInputStream.read(dataBytes)) != -1) {
				System.out.println("nread: "+nread);
				String head = ""+Values.backup_chunk_data_message_identifier+" 1.0 "+fileIdentifier+" "+chunkNum+" "+replicationDegree;
				System.out.println(head);
				ProtocolMessage protocolMessage = new ProtocolMessage(head, dataBytes);
				DatagramPacket packet = new DatagramPacket(dataBytes, dataBytes.length, Values.multicast_backup_group_address, Values.multicast_backup_group_port);
				System.out.println(packet.getData().length);
				BackupChannelThread.getMulticast_backup_socket().send(packet);
				System.out.println("Sent");
				chunkNum++;
			}

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}



	}




	/**
	 * Run_threads.
	 */
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

	/**
	 * Initialize_sockets.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void initialize_sockets() throws IOException{

		ControlChannelThread.init_socket();
		BackupChannelThread.init_socket();
		RestoreChannelThread.init_socket();

	}

}