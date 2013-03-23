package server;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;

import protocols.*;
import com.google.gson.*;

import constantValues.Values;


public class Server{

	private static ControlChannelThread control_thread;
	private static BackupChannelThread backup_thread;
	private static RestoreChannelThread restore_thread;
	private static InetAddress thisMachineAddress;
	private static ArrayList<InetAddress> machineAddresses;
	private static Server instance;
	
	public static Random rand;
	
	private HashMap<String,DatagramPacket> packets_sent;
	private Config config;
	private BufferedReader bufferedReader;
	private HashMap<String,Boolean> backedUpPaths;
	
	
	private Server() {
	    packets_sent = new HashMap<String,DatagramPacket>();
	    backedUpPaths = new HashMap<String,Boolean>();
	    Server.rand = new Random();
	    Server.machineAddresses = new ArrayList<InetAddress>();
        Server.thisMachineAddress = null;
        bufferedReader = null;
        
        // not a very elegant solution, but it works
        Enumeration<NetworkInterface> nets;
        
        
        try {
            nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets)) {
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                for (InetAddress inetAddress : Collections.list(inetAddresses)) {
                    Server.machineAddresses.add(inetAddress);
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
	}
	
	public static Server getInstance() {
	    if(instance == null) {
	        instance = new Server();
	    }
	    return instance;
	}

	public void mainLoop() {
		run_threads();
		
		Gson gson = new Gson();

        try {
            bufferedReader = new BufferedReader(new FileReader("config.json"));
            config = gson.fromJson(bufferedReader, Config.class);
        } catch (FileNotFoundException e) {
            System.out.println("Configuration file is missing. Shutting down the server.");
            System.exit(-1);
        }

        bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        
		while(true) {
		    System.out.println("-----------   BACKUP SERVICE   -----------\n");
		    System.out.println(" 1 - Backup files in config.json");
		    System.out.println(" 2 - Backup new file");
		    System.out.println(" 3 - List backed files");
		    System.out.println(" 4 - Restore file");
		    System.out.println(" 5 - Delete file");
		    System.out.println(" 0 - Exit");
		    System.out.println("\n------------------------------------------");
		    System.out.print("\nOption: ");
		    
		    String userInput;
            try {
                userInput = bufferedReader.readLine();
                
                switch (userInput) {
                case "1": {
                    backupConfigFiles();
                }
                    break;
                case "2": {
                    backupNewFile();
                }
                    break;
                case "3": {
                    listBackedFiles();
                }
                    break;
                case "4": {
                    restoreFile();
                }
                    break;
                case "5": {
                    deleteFile();
                }
                    break;
                case "0": {
                    System.exit(-1);
                }
                    break;
                default:
                    break;
                }
                
            } catch (IOException e) {
                e.printStackTrace();
            }
		}
	}
	
	private void listBackedFiles() {
	    Iterator<Entry<String,Boolean>> it = backedUpPaths.entrySet().iterator();
	    int counter = 1;
	    while (it.hasNext()) {
            Map.Entry<String,Boolean> pair = (Map.Entry<String,Boolean>)it.next();
            if(pair.getValue()){ // is backed up in another peer
                System.out.printf("%d - %s\n",counter++,pair.getKey());
            }
	    }
	}

	private void deleteFile() {
	    try {
	        
	        System.out.print("Path of file: ");
	        String filePath = bufferedReader.readLine();

	        final File file = new File(filePath);

	        if(file.exists()) {
	            if(!file.isDirectory()) {
	                new Thread(new Runnable() {
	                    @Override
	                    public void run() {
	                        String fileIdentifier = HashString.getFileIdentifier(file);
	                        String head = Values.file_deleted_control_message_identifier + " "
	                                + Values.protocol_version + " "
	                                + fileIdentifier;

	                        byte[] buf = ProtocolMessage.toBytes(head, null);
	                        DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_control_group_address, Values.multicast_control_group_port);

	                        for(int i = 0; i < Values.number_of_attempts_to_delete_files; i++) {
	                            try {
	                                Thread.sleep(i*50);
	                                ControlChannelThread.getMulticast_control_socket().send(packet);
	                            } catch (InterruptedException | IOException e) {
	                                e.printStackTrace();
	                            }
	                        }
	                    }
	                }).start();
	            } else {
	                System.out.println("WE'RE NOT ACCEPTING DIRECTORIES FOR NOW. INDIVIDUAL FILES ONLY."); // TODO
	            }
	        } else {
	            System.out.println("THAT FILE DOESN'T EXIST! TRY AGAIN.");
	        }

	    } catch (Exception e) {

	    }
	}

	private void restoreFile() {
	    File file = new File("walking/tiger.jpg");
	    String fileIdentifier = HashString.getFileIdentifier(file);
	    
	    String head = Values.send_chunk_data_message_identifier + " "
	            + Values.protocol_version + " "
	            + fileIdentifier + " "
	            + 0;

	    byte[] buf = ProtocolMessage.toBytes(head, null);
	    DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_control_group_address, Values.multicast_control_group_port);
	    
	    try {
            ControlChannelThread.getMulticast_control_socket().send(packet);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	}

	private void backupConfigFiles() {
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
	
	private void backupNewFile() {
	    try {
	        System.out.print("Path of file: ");
            String filePath = bufferedReader.readLine();
            
            File file = new File(filePath);
            if(file.exists()) {
                if(!file.isDirectory()) {
                    System.out.print("Replication degree (between 1 and 9): ");
                    String replicationDegreeStr = bufferedReader.readLine();
                    String pattern = "^[1-9]$";
                    if(replicationDegreeStr.matches(pattern)) {
                        String fileId = HashString.getFileIdentifier(file);
                        int numberChunks = (int)Math.ceil(file.length()/64000.0);
                        System.out.println("FileId = "+fileId);
                        System.out.println("Chunks = "+numberChunks);
                       
                        process_file(file, Integer.parseInt(replicationDegreeStr));
                        
                        for(int i = 0; i < numberChunks; i++) {
                            send_file(fileId, String.valueOf(i));
                        }
                    } else {
                        System.out.println("Invalid replication degree.");
                    }
                } else {
                    System.out.println("You can only backup individual files.");
                }
            } else {
                System.out.println("The file doesn't exist. Check the path and try again.");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	    
	}

	private void process_directory(File directory, int replicationDegree) {
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

	private void process_file(final File file, final int replicationDegree) {

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
				
				Server.control_thread.updateRequestedBackups(new Header(head));
				backedUpPaths.put(file.getAbsolutePath(), false);
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
	        int delay = Server.rand.nextInt(201)+Values.server_sending_packets_delay;
	        try {
	            Thread.sleep(delay);
	            BackupChannelThread.getMulticast_backup_socket().send(pair.getValue());
	            System.out.println("----------------Sent PUTCHUNK message----------------\n");
	        } catch (InterruptedException | IOException e) {
	            e.printStackTrace();
	            // TODO what to do here?
	        }
	    }
	    this.getControl_thread().notifyDaemonSupervisor();//Some improving needs to be done
	}

	public synchronized void send_file(String fileId, String chunkNumber) {
	    String key = fileId + ":" + chunkNumber;
	    if(packets_sent.containsKey(key)){
	        int delay = Server.rand.nextInt(201)+Values.server_sending_packets_delay;
	        try {
	            Thread.sleep(delay);
	            BackupChannelThread.getMulticast_backup_socket().send(packets_sent.get(key));
	            System.out.println("----------------Sent PUTCHUNK message----------------\n");
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
	private void run_threads(){

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
		Server.control_thread = control_thread;
	}
	public BackupChannelThread getBackup_thread() {
		return backup_thread;
	}
	public void setBackup_thread(BackupChannelThread backup_thread) {
		Server.backup_thread = backup_thread;
	}
	public RestoreChannelThread getRestore_thread() {
		return restore_thread;
	}
	public void setRestore_thread(RestoreChannelThread restore_thread) {
		Server.restore_thread = restore_thread;
	}

	
	private class FileToBackup {
        public String path;
        public int replicationDegree;
    }

    private class Config {
        public int availableSpaceOnServer;
        public ArrayList<FileToBackup> filesToBackup;
    }
}
