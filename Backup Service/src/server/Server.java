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
	}

	public void mainLoop() {
		run_threads();

		Gson gson = new Gson();
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
		}
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

		String fileIdentifier = HashString.getFileIdentifier(file);
		
			
		try {
			
			FileInputStream fileInputStream = new FileInputStream(file);
			byte[] dataBytes = new byte[64000];
			int chunkSize = -1;
			int chunkNum = 0;
			
			System.out.println("\n\n------------------------SENDING FILE------------------------\n"
					+ "\nFilename: " + file.getName() 
					+ "\nSize: " + file.length() 
					+ "\nProtocol version: " + Values.protocol_version
					+ "\nFile identifier: " + fileIdentifier
					+ "\nReplication degree: " + replicationDegree
					+ "\nChunks: " + (int)Math.ceil(file.length()/64000.0));
			
			while ((chunkSize = fileInputStream.read(dataBytes)) != -1){ //read from file into dataBytes
				
			    if(chunkSize < 64000) {
			        byte[] temp = new byte[chunkSize];
			        System.arraycopy(dataBytes, 0, temp, 0, chunkSize);
			        dataBytes = temp;
			    }
				
				System.out.println("\nSENDING CHUNK NUMBER: " + chunkNum);
				System.out.println("Chunk size: " + chunkSize);
				
				String head = Values.backup_chunk_data_message_identifier + " "
									+ Values.protocol_version + " "
									+ fileIdentifier + " "
									+ chunkNum + " "
									+ replicationDegree;
				
				//System.out.println("HEADER: " + head);
				
				this.control_thread.updateRequestedBackups(new Header(head));
				
				
				byte[] buf = ProtocolMessage.toBytes(head, dataBytes);
				
				DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_backup_group_address, Values.multicast_backup_group_port);
				BackupChannelThread.getMulticast_backup_socket().send(packet);
				
				System.out.println("\nSent " + packet.getLength() + " bytes");
				System.out.println("------------------------Chunk sent------------------------\n\n");
				chunkNum++;
			}
			
			fileInputStream.close();

		} catch (IOException e) {
			e.printStackTrace();
			//System.exit(-1);
		}
	}


	
	/**
	 * Run_threads.
	 */
	public void run_threads(){

		Thread.currentThread().setName("MainThread");
		control_thread = new ControlChannelThread(); // TODO change to singleton pattern
		backup_thread = BackupChannelThread.getInstance();
		restore_thread = new RestoreChannelThread(); // TODO change to singleton pattern

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

	public ControlChannelThread getControl_thread() {
		return control_thread;
	}
	public void setControl_thread(ControlChannelThread control_thread) {
		this.control_thread = control_thread;
	}
	public BackupChannelThread getBackup_thread() {
		return backup_thread;
	}
	public void setBackup_thread(BackupChannelThread backup_thread) {
		this.backup_thread = backup_thread;
	}
	public RestoreChannelThread getRestore_thread() {
		return restore_thread;
	}
	public void setRestore_thread(RestoreChannelThread restore_thread) {
		this.restore_thread = restore_thread;
	}

}