package server;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;

import protocols.*;
import com.google.gson.*;

import constantValues.Values;


public class Server{

	private ControlChannelThread control_thread;
	private BackupChannelThread backup_thread;
	private RestoreChannelThread restore_thread;
	private HashMap<String,DatagramPacket> packets_sent;
	public static Random rand;
	private static ArrayList<InetAddress> machineAddresses;
	private static InetAddress thisMachineAddress;

	public class FileToBackup {
	    public String path;
	    public int replicationDegree;
	}

	public class Config {
		public int availableSpaceOnServer;
		public ArrayList<FileToBackup> filesToBackup;
	}
	
	public Server() {
	    packets_sent = new HashMap<String,DatagramPacket>();
	    rand = new Random();
	    machineAddresses = new ArrayList<InetAddress>();
        thisMachineAddress = null;
        
        // not a very elegant solution, but it works
        Enumeration<NetworkInterface> nets;
        try {
            nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets)) {
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                for (InetAddress inetAddress : Collections.list(inetAddresses)) {
                    machineAddresses.add(inetAddress);
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
	}

	public void mainLoop() {
		run_threads();

		Gson gson = new Gson();
		BufferedReader bufferedReader = null;

		try {
			bufferedReader = new BufferedReader(new FileReader("config.json"));
		} catch (FileNotFoundException e) {
			//e.printStackTrace();
			System.out.println("Configuration file is missing. Shutting down the server.");
			System.exit(-1);
		}

		Config config = gson.fromJson(bufferedReader, Config.class);

		// go through each file and build the packets
		for(FileToBackup f : config.filesToBackup) {
			File file = new File(f.path);
			if(file.exists()) {
				if(file.isDirectory()) {
					process_directory(file, f.replicationDegree);
				} else {
					process_file(file, f.replicationDegree);

				}
			} else {
				System.out.println("\nThe file/dir "+file.getAbsolutePath()+" doesn't exist!");
			}
		}
		System.out.println("\n\n----------------------FINISHED PROCESSING FILES------------------------\n");
		send_files();
	}

	public void process_directory(File directory, int replicationDegree) {
		File[] files = directory.listFiles();
		for(File f : files) {
			if(f.isDirectory()) {
				process_directory(f, replicationDegree);
			} else {
				if(!f.isHidden()) {
					process_file(f, replicationDegree);
				}
			}
		}
	}

	public void process_file(final File file, final int replicationDegree) {

		String fileIdentifier = HashString.getFileIdentifier(file);
			
		try {
			FileInputStream fileInputStream = new FileInputStream(file);
			byte[] dataBytes = new byte[64000];
			int chunkSize = -1;
			int chunkNum = 0;
			
			System.out.println("\n\n------------------------PROCESSING NEW FILE------------------------\n"
					+ "\nFilename: " + file.getName() 
					+ "\nSize: " + file.length() 
					+ "\nProtocol version: " + Values.protocol_version
					+ "\nFile identifier: " + fileIdentifier
					+ "\nReplication degree: " + replicationDegree
					+ "\nChunks: " + (int)Math.ceil(file.length()/64000.0)
					+ "\n");
			
			while ((chunkSize = fileInputStream.read(dataBytes)) != -1){
				
			    if(chunkSize < 64000) {
			        byte[] temp = new byte[chunkSize];
			        System.arraycopy(dataBytes, 0, temp, 0, chunkSize);
			        dataBytes = temp;
			    }
				
				System.out.print("CREATING CHUNK #" + chunkNum);
				System.out.println(" WITH SIZE: " + chunkSize + " BYTES");
				
				String head = Values.backup_chunk_data_message_identifier + " "
									+ Values.protocol_version + " "
									+ fileIdentifier + " "
									+ chunkNum + " "
									+ replicationDegree;
				
				this.control_thread.updateRequestedBackups(new Header(head));
				byte[] buf = ProtocolMessage.toBytes(head, dataBytes);
				
				DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_backup_group_address, Values.multicast_backup_group_port);
				packets_sent.put(fileIdentifier+":"+chunkNum, packet);
				chunkNum++;
			}
			fileInputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
			//System.exit(-1);
		}
	}
	
	private void send_files() {
	    Iterator<Entry<String,DatagramPacket>> it = packets_sent.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String,DatagramPacket> pair = (Map.Entry<String,DatagramPacket>)it.next();
	        int delay = Server.rand.nextInt(301)+Values.server_sending_packets_delay;
	        try {
	            Thread.sleep(delay);
	            BackupChannelThread.getMulticast_backup_socket().send(pair.getValue());
	        } catch (InterruptedException | IOException e) {
	            e.printStackTrace();
	            // TODO what to do here?
	        }
	    }
	}

	public synchronized void send_file(String fileId, String chunkNumber) {
	    String key = fileId + ":" + chunkNumber;
	    if(packets_sent.containsKey(key)){
	        int delay = Server.rand.nextInt(301)+Values.server_sending_packets_delay;
	        try {
	            Thread.sleep(delay);
	            BackupChannelThread.getMulticast_backup_socket().send(packets_sent.get(key));
	        } catch (IOException | InterruptedException e) {
	            e.printStackTrace();
	            // TODO what to do here?
	        }
	    }
	}
	
	public static boolean fromThisMachine(InetAddress src){
        if(thisMachineAddress == null) {
            for(InetAddress a : machineAddresses) {
                if(a.getHostAddress().compareTo(src.getHostAddress()) == 0) {
                    thisMachineAddress = a;
                    return true;
                }
            }
            return false;
        } else {
            return (thisMachineAddress.getHostAddress().compareTo(src.getHostAddress()) == 0);
        }
    }

	/**
	 * Run_threads.
	 */
	public void run_threads(){

		Thread.currentThread().setName("MainThread");
		control_thread = ControlChannelThread.getInstance();
		backup_thread = BackupChannelThread.getInstance();
		restore_thread = RestoreChannelThread.getInstance();

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