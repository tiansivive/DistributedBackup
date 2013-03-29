package server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Type;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.rowset.spi.SyncResolver;

import protocols.Header;
import protocols.ProtocolMessage;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import constantValues.Values;


public class Server{

	private static ControlChannelThread control_thread;
	private static BackupChannelThread backup_thread;
	private static RestoreChannelThread restore_thread;
	private static InetAddress thisMachineAddress;
	private static ArrayList<InetAddress> machineAddresses;
	private static Server instance;
	
	public static Random rand;
	
	private HashMap<String,DatagramPacket> packetsQueue;
	private int numberOfChunksProcessed;
	private Config config;
	private BufferedReader bufferedReader;
	private HashMap<String,BackedUpFile> backedUpFiles;
	private HashSet<String> replicasRemovedFromOtherMachines; 
	private boolean hasBackedUpConfigFiles;
	private Gson gson;
	private boolean isReclaimingSpace;
	
	private Server() {
	    packetsQueue = new HashMap<String,DatagramPacket>();
	    backedUpFiles = new HashMap<String,BackedUpFile>();
	    replicasRemovedFromOtherMachines = new HashSet<String>();
	    Server.rand = new Random();
	    Server.machineAddresses = new ArrayList<InetAddress>();
        Server.thisMachineAddress = null;
        bufferedReader = null;
        numberOfChunksProcessed = 0;
        hasBackedUpConfigFiles = false;
        isReclaimingSpace = false;
        gson = new Gson();
        
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
        
        try {
            bufferedReader = new BufferedReader(new FileReader("config.json"));
            config = gson.fromJson(bufferedReader, Config.class);
            
        } catch (FileNotFoundException e) {
            System.out.println("Configuration file is missing. Shutting down the server.");
            System.exit(-1);
        }

        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(new Runnable() {
        	@Override
        	public void run() {
        		//System.out.println("SERVER - SAVING CONFIG AND BACKEDUP FILES");
        		saveConfigToJson();
        		saveBackedUpFilesToJson();
        	}
        }, 0, 10, TimeUnit.SECONDS);
	}
	
	public static Server getInstance() {
	    if(instance == null) {
	        instance = new Server();
	    }
	    return instance;
	}
	
	private void loadBackedUpFiles() {
		try {
			bufferedReader = new BufferedReader(new FileReader("backedUpFiles.json"));
			Type stringBackedUpFile = new TypeToken<HashMap<String,BackedUpFile>>(){}.getType();
			backedUpFiles = gson.fromJson(bufferedReader, stringBackedUpFile);
        } catch (FileNotFoundException e) {
        	// ignore, there weren't any backed up files
        }
	}

	public void mainLoop() {
		
        loadBackedUpFiles();
        run_threads();
        createNecessaryFiles();

        bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        
		while(true) {
		    System.out.println("-----------   BACKUP SERVICE   -----------\n");
		    if(!hasBackedUpConfigFiles) {
		        System.out.println(" 1 - Backup files in config.json");
		    }
		    System.out.println(" 2 - Backup new file");
		    System.out.println(" 3 - List backed files");
		    System.out.println(" 4 - Restore file");
		    System.out.println(" 5 - Delete file");
		    System.out.println(" 6 - Reclaim Space");
		    System.out.println(" 0 - Exit");
		    System.out.println("\n------------------------------------------");
		    System.out.print("\nOption: ");
		    
		    String userInput;
            try {
                userInput = bufferedReader.readLine();
                
                switch (userInput) {
                case "1": {
                    if(!hasBackedUpConfigFiles) {
                        hasBackedUpConfigFiles = true;
                        backupConfigFiles();
                    }
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
                case "6": {
                    reclaimSpace();
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

	private void reclaimSpace() throws IOException {

		String userInput;
		int spaceToReclaim;
		while(true){
			System.out.println("\nHow many KB? (-1 to cancel) ");
			userInput = bufferedReader.readLine();
			try{
				spaceToReclaim = Integer.parseInt(userInput);
				break;
			}catch(NumberFormatException e){
				System.out.println("Input not a number, please try again");
			}
		}

		if(spaceToReclaim != -1) {
			spaceToReclaim = spaceToReclaim * 1000; //Number of bytes
			int amountOfSpaceReclaimed = 0;

			boolean onlySelectChunksWithMoreThanDesiredReplication = true;
			isReclaimingSpace = true;
			
			Map<String, ArrayList<Integer>> chunksToBeRemoved = new HashMap<String,ArrayList<Integer>>();
			synchronized (getControl_thread().getReplicationDegreeOfOthersChunks()) {

				HashMap<String, Map<Integer,Integer>> tmp = getControl_thread().getReplicationDegreeOfOthersChunks();
				String fileSeparator = System.getProperty("file.separator");
				
				Iterator<String> fileIterator;
				if(tmp.size() > 0) {
					while(true){
						
						fileIterator = tmp.keySet().iterator();
						System.out.println("WHILE TRUE");
						while(fileIterator.hasNext()){

							ArrayList<Integer> chunksSurplus = new ArrayList<Integer>();
							String fileID = (String)fileIterator.next();
							System.out.println(fileID);
							Iterator<Integer> chunksIterator = tmp.get(fileID).keySet().iterator();    
							

							while(chunksIterator.hasNext()){
								
								Integer chunkNum = chunksIterator.next();
								System.out.println("CHUNKNUM = " + chunkNum);
								boolean addItToTheList = false;
								
								if(onlySelectChunksWithMoreThanDesiredReplication) {
									if(getControl_thread().hasChunkGotMoreThanDesiredNumberOfReplicas(fileID, chunkNum)) {
										addItToTheList = true;
									}
								} else {
									addItToTheList = true;
								}
								
								if(addItToTheList) {	
									synchronized (getBackup_thread().getBackedFiles()) {
										ArrayList<Integer> tmpChunks = getBackup_thread().getBackedFiles().get(fileID);
										if(tmpChunks != null) {
											if(tmpChunks.contains(chunkNum)) {
												File chunk = new File(Values.directory_to_backup_files + fileSeparator 
														+ fileID + fileSeparator
														+ "chunk_" + chunkNum);
												amountOfSpaceReclaimed += chunk.length();

												System.out.println("ADDING CHUNK NUMBER " + chunkNum 
														+ " FROM FILE " + fileID 
														+ " TO REMOVE LIST");  
												chunksSurplus.add(chunkNum);
											}
										}
									}
								}

								if(amountOfSpaceReclaimed >= spaceToReclaim){
									break;
								}
							}

							if(!chunksSurplus.isEmpty()){
								
								ArrayList<Integer> temp = chunksToBeRemoved.get(fileID);
								
								if(temp != null){
									for(int i = 0; i < chunksSurplus.size(); i++){
										temp.add(chunksSurplus.get(i));
									}
									chunksToBeRemoved.put(fileID, temp);
								}else{
									chunksToBeRemoved.put(fileID, chunksSurplus);
								}
							}
							
							if(amountOfSpaceReclaimed >= spaceToReclaim){
								break;
							}
							System.out.println("FINISHED CHUNK ITERATION CICLE");
						}

						if(amountOfSpaceReclaimed >= spaceToReclaim){//DON'T DELETE MORE THAN NEEDED
							break;
						}else{
							onlySelectChunksWithMoreThanDesiredReplication = false;
						}
						
						System.out.println("FINISHED FILE ITERATION CICLE");
					}
					System.out.println("GOING TO REMOVE " + amountOfSpaceReclaimed + " BYTES");
					
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					fileIterator = chunksToBeRemoved.keySet().iterator();
					while(fileIterator.hasNext()){
						
						String fileID = (String)fileIterator.next();
						Iterator<Integer> chunksIterator = chunksToBeRemoved.get(fileID).iterator();
						
						while(chunksIterator.hasNext()){
							
							int chunkNum = (int)chunksIterator.next();
							int currentReplication = tmp.get(fileID).get(chunkNum);
							currentReplication--;
							tmp.get(fileID).put(chunkNum, currentReplication);

							File chunk = new File(Values.directory_to_backup_files + fileSeparator 
									+ fileID + fileSeparator
									+ "chunk_" + chunkNum);		
							
							if(chunk.delete()) {
								synchronized (getBackup_thread().getBackedFiles()) {
									ArrayList<Integer> tmpChunks = getBackup_thread().getBackedFiles().get(fileID);
									if(tmpChunks != null) {
										tmpChunks.remove(chunkNum);
									}
								}
								getBackup_thread().send_REMOVED_messageForChunk(fileID, chunkNum);
							}
							chunksIterator.remove();
						}
						File fileDir = new File(Values.directory_to_backup_files + fileSeparator + fileID);
						if(fileDir.delete()) {
							synchronized (getBackup_thread().getBackedFiles()) {
								getBackup_thread().getBackedFiles().remove(fileID);
							}
						}
					}
					System.out.println("DONE RECLAIMING SPACE");
					config.currentAvailableSpaceOnServer += amountOfSpaceReclaimed;
					isReclaimingSpace = false; // TODO NEEDS IMPROVEMENTS
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void createNecessaryFiles() {

		HashMap<String, Integer> toInitializeDesiredReplication = new HashMap<String,Integer>();
		HashMap<String, Map<Integer,Integer>> toInitializeReplicationDegree = new HashMap<String, Map<Integer,Integer>>();

		synchronized(getControl_thread().getReplicationDegreeOfOthersChunks()){
			try
			{
				FileInputStream fileIn = new FileInputStream("ReplicationDegreeOfOthersChunks.FAP");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				toInitializeReplicationDegree = (HashMap<String,Map<Integer,Integer>>) in.readObject();
				getControl_thread().setReplicationDegreeOfOthersChunks(toInitializeReplicationDegree);
				in.close();
				fileIn.close();
				System.out.println("Loaded ReplicationDegreeOfOthersChunks into memory");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch(FileNotFoundException e){
				try
				{
					FileOutputStream fileOut = new FileOutputStream("ReplicationDegreeOfOthersChunks.FAP");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(toInitializeDesiredReplication);
					out.close();
					fileOut.close();
				} catch(IOException i) {
					i.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		synchronized (getControl_thread().getDesiredReplicationOfFiles()) {
			try
			{
				FileInputStream fileIn = new FileInputStream("DesiredReplicationOfFiles.FAP");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				toInitializeDesiredReplication = (HashMap<String,Integer>) in.readObject();
				getControl_thread().setDesiredReplicationOfFiles(toInitializeDesiredReplication);
				in.close();
				fileIn.close();
				System.out.println("Loaded DesiredReplicationOfFiles into memory");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch(FileNotFoundException e){
				try
				{
					FileOutputStream fileOut = new FileOutputStream("DesiredReplicationOfFiles.FAP");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(toInitializeDesiredReplication);
					out.close();
					fileOut.close();
				} catch(IOException i) {
					i.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void listBackedFiles() {
	    Iterator<Entry<String,BackedUpFile>> it = backedUpFiles.entrySet().iterator();
	    
	    if(backedUpFiles.size() > 0) {
	    	System.out.println("\n----------   BACKED UP FILES   ----------\n");
	    	while (it.hasNext()) {
	    		Map.Entry<String,BackedUpFile> pair = (Map.Entry<String,BackedUpFile>)it.next();
	    		System.out.println(pair.getValue().path);
	    	}
	    } else {
	    	System.out.println("\nYou don't have files backed up");
	    }
	    
	    bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("\n\nPRESS ANY KEY TO CONTINUE");
        try {
			bufferedReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
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
	                System.out.println("WE'RE NOT ACCEPTING DIRECTORIES FOR NOW. INDIVIDUAL FILES ONLY.");
	            }
	        } else {
	            System.out.println("THAT FILE DOESN'T EXIST! TRY AGAIN.");
	        }

	    } catch (Exception e) {

	    }
	}

	private void restoreFile() {

	    System.out.print("File name contains: ");
	    String substr;
	    boolean atLeastOneMatch = false;
	    
	    try {
	        substr = bufferedReader.readLine();

	        ArrayList<BackedUpFile> matchedBackedUpFiles = new ArrayList<BackedUpFile>();
	        Iterator<Entry<String,BackedUpFile>> it = backedUpFiles.entrySet().iterator();

	        while (it.hasNext()) {
	            Map.Entry<String,BackedUpFile> pair = (Map.Entry<String,BackedUpFile>)it.next();
	            if(pair.getValue().path.contains(substr)) {
	                System.out.println(matchedBackedUpFiles.size()+" - "+pair.getValue().path);
	                matchedBackedUpFiles.add(pair.getValue());
	                if(!atLeastOneMatch) {
	                    atLeastOneMatch = true;
	                }
	            }
	        }
	        
	        if(atLeastOneMatch) {
	            ArrayList<Integer> indexes = new ArrayList<Integer>();
                boolean continueRequest = false;
                
	            System.out.print("Insert index (-1 to cancel): ");
	            String input = bufferedReader.readLine();
	            
	            try {
	                int pathIndex = Integer.parseInt(input);
	                if(pathIndex > -1) {
	                    indexes.add(pathIndex);
	                    continueRequest = true;
	                }
	            } catch (NumberFormatException e) {
	                String pattern = "[0-9]{1,}(-[0-9]{1,})*"; // indexes separated by '-'
	                if(input.matches(pattern)) {
	                    String[] fields = input.split("-");
	                    for(String indexStr : fields) {
	                        indexes.add(Integer.parseInt(indexStr));
	                    }
	                    continueRequest = true;
	                } else {
	                    System.out.println("INVALID INPUT");
	                }
	            }

	            if(continueRequest) {
	                for(Integer index : indexes) {
	                    BackedUpFile file = matchedBackedUpFiles.get(index);

	                    getRestore_thread().addRequestForFileRestoration(file.fileId,file.path,file.numberOfChunks);
	                    for(int i = 0; i < file.numberOfChunks; i++) {
	                        String head = Values.recover_chunk_control_message_identifier + " "
	                                + Values.protocol_version + " "
	                                + file.fileId + " "
	                                + i;

	                        byte[] buf = ProtocolMessage.toBytes(head, null);
	                        DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_control_group_address, Values.multicast_control_group_port);
	                        ControlChannelThread.getMulticast_control_socket().send(packet);
	                        Thread.sleep(Values.server_sending_packets_delay);
	                    }
	                }
	                getRestore_thread().notifyDaemonSupervisor();
	            }
	        } else {
	            System.out.println("NO MATCHES. TRY AGAIN.\n");
	        }
	    } catch (IOException | InterruptedException e1) {
	        e1.printStackTrace();
	    }
	}

	public void hasReachedMinimumReplicationDegree(String fileId) {
	    if(backedUpFiles.containsKey(fileId)) {
	        backedUpFiles.get(fileId).hasAtLeastOneReplica = true;
	        //saveBackedUpFilesToJson();
	    }
	}
	
	private void saveBackedUpFilesToJson() {
        String json = gson.toJson(backedUpFiles);

        try {
            FileWriter writer = new FileWriter("backedUpFiles.json");
            writer.write(json);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	private void backupConfigFiles() {
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
	    if(numberOfChunksProcessed != 0) {
	        send_files();
	        numberOfChunksProcessed = 0;
	        packetsQueue.clear();
	    }
	    System.out.println("----------------------FINISHED PROCESSING FILES------------------------");
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
                        
                        if(numberOfChunksProcessed != 0) {
                            send_files();
                            numberOfChunksProcessed = 0;
                            packetsQueue.clear();
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
	        int numberChunks;

	        System.out.println("\n\n------------------------PROCESSING NEW FILE------------------------\n"
	                + "\nFilename: " + file.getName() 
	                + "\nSize: " + file.length() 
	                + "\nProtocol version: " + Values.protocol_version
	                + "\nFile identifier: " + fileIdentifier
	                + "\nReplication degree: " + replicationDegree
	                + "\nChunks: " + (numberChunks = (int)Math.ceil(file.length()/64000.0))
	                + "\n");
	        
	        backedUpFiles.put(fileIdentifier,new BackedUpFile(fileIdentifier,file.getAbsolutePath(), false, numberChunks));
	        getControl_thread().setFilesDesiredReplication(fileIdentifier, replicationDegree);
	        
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
                byte[] buf = ProtocolMessage.toBytes(head, dataBytes);

                DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_backup_group_address, Values.multicast_backup_group_port);
                packetsQueue.put(fileIdentifier+":"+chunkNum, packet);
                chunkNum++;
	            
                if(++numberOfChunksProcessed == Values.max_number_chunks_sent_simultaneously) {
                    synchronized (this) {
                        send_files();
                        System.out.println(Thread.currentThread().getName()+" WAITING");
                        wait();
                        numberOfChunksProcessed = 0;
                        packetsQueue.clear();
                    }
                }
	        }
	        fileInputStream.close();
	    } catch (IOException | InterruptedException e) {
	        e.printStackTrace();
	    }
	}

	private void send_files() {
	    Iterator<Entry<String,DatagramPacket>> it = packetsQueue.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String,DatagramPacket> pair = (Map.Entry<String,DatagramPacket>)it.next();
	        int delay = Server.rand.nextInt(Values.server_sending_packets_delay +1);
	        try {
	            Thread.sleep(delay);
	            BackupChannelThread.getMulticast_backup_socket().send(pair.getValue());
	            System.out.println("----------------Sent PUTCHUNK message----------------");
	        } catch (InterruptedException | IOException e) {
	            e.printStackTrace();
	            // TODO what to do here?
	        }
	    }
	    getControl_thread().notifyDaemonSupervisor();
	}

	public void send_file(String fileId, String chunkNumber) {
	    synchronized (packetsQueue) {
	        String key = fileId + ":" + chunkNumber;
	        if(packetsQueue.containsKey(key)){
	            int delay = Server.rand.nextInt(+Values.server_sending_packets_delay+1);
	            try {
	                Thread.sleep(delay);
	                BackupChannelThread.getMulticast_backup_socket().send(packetsQueue.get(key));
	                System.out.println("----------------Sent PUTCHUNK message----------------");
	            } catch (IOException | InterruptedException e) {
	                e.printStackTrace();
	            }
	        }
        }
	}
	
	public void buildPacketFrom_REMOVED_Message(Header message, int desiredReplication) throws IOException {
		
	
		if(this.replicasRemovedFromOtherMachines.contains(message.getFileID()+":"+message.getChunkNumber())){
			synchronized (replicasRemovedFromOtherMachines) {
				this.replicasRemovedFromOtherMachines.remove(message.getFileID()+":"+message.getChunkNumber());
			}
			
			String head = Values.backup_chunk_data_message_identifier + " "
					+ Values.protocol_version + " "
					+ message.getFileID() + " "
					+ message.getChunkNumber() + " "
					+ desiredReplication;
			
			System.out.println("\nCREATED THIS MESSAGE FROM REMOVED MERSSAGE: " + head + "\n");

			String fileSeparator = System.getProperty("file.separator");
			File chunk = new File(Values.directory_to_backup_files + fileSeparator
					+ message.getFileID() + fileSeparator
					+ "chunk_" + message.getChunkNumber());

			if(chunk.exists()) {
				byte[] chunkData = new byte[Values.number_of_bytes_in_chunk];

				FileInputStream input = new FileInputStream(chunk);
				int chunkSize = input.read(chunkData);
				input.close();
				if(chunkSize < Values.number_of_bytes_in_chunk) {
					byte[] temp = new byte[chunkSize];
					System.arraycopy(chunkData, 0, temp, 0, chunkSize);
					chunkData = temp;
				}

				byte[] buf = ProtocolMessage.toBytes(head, chunkData);
				DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_backup_group_address, Values.multicast_backup_group_port);
				packetsQueue.put(message.getFileID()+":"+message.getChunkNumber(), packet);
				getControl_thread().updateRequestedBackups(new Header(head));
				getControl_thread().updateOutRequestedBackupsCurrentReplication(message.getFileID(), message.getChunkNumber());
				BackupChannelThread.getMulticast_backup_socket().send(packet);	
				System.out.println("\n----------------SENT PUTCHUNK MESSAGE BECAUSE A REMOVED MESSAGE WAS RECEIVED----------------\n");
				try {
					Thread.sleep(Values.default_supervisor_delay);
					getControl_thread().notifyDaemonSupervisor();
				} catch (InterruptedException e) {				
					e.printStackTrace();
				}
			}else{
				System.out.println("FILE NOT FOUND");
			}
		}else{
			System.out.println("SOMEBODY ALREADY SENT THE PUTCHUNK REPONSE TO A REMOVED MESSAGE!");
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
		control_thread = ControlChannelThread.getInstance(this);
		backup_thread = BackupChannelThread.getInstance(this);
		restore_thread = RestoreChannelThread.getInstance(this);

		control_thread.start();
		backup_thread.start();
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
	
	public long getAvailableSpaceOnServer() {
		return config.currentAvailableSpaceOnServer;
	}
	
	public boolean isReclaimingSpace() {
		return isReclaimingSpace;
	}
	
	public void removeThisSpaceFromServer(int bytes) {
		config.currentAvailableSpaceOnServer -= bytes;
		//System.out.println("REMOVED "+bytes+" BYTES FROM THE AVAILABLE SPACE!");
	}
	
	private void saveConfigToJson() {
		synchronized (config) {
			String json = gson.toJson(config);

	        try {
	            FileWriter writer = new FileWriter("config.json");
	            writer.write(json);
	            writer.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
		}
	}

	public void addRemovedMessageInfomation(String fileID, String chunkNum){
		synchronized (replicasRemovedFromOtherMachines) {
			this.replicasRemovedFromOtherMachines.add(fileID+":"+chunkNum);
		}
	}
	
	public String getProtocolVersion() {
		return config.protocolVersion;
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

	public HashSet<String> getReplicasRemovedFromOtherMachines() {
		return replicasRemovedFromOtherMachines;
	}

	public void setReplicasRemovedFromOtherMachines(
			HashSet<String> replicasRemovedFromOtherMachines) {
		this.replicasRemovedFromOtherMachines = replicasRemovedFromOtherMachines;
	}

	private class BackedUpFile {
	    public String path;
	    public String fileId;
	    public boolean hasAtLeastOneReplica;
	    public int numberOfChunks;
	    public BackedUpFile(String fileId, String path, boolean hator, int numberChunks) {
	        this.fileId = fileId;
	        this.path = path;
	        this.hasAtLeastOneReplica = hator;
	        this.numberOfChunks = numberChunks;
	    }
	}
	
	private class FileToBackup {
        public String path;
        public int replicationDegree;
    }

    private class Config {
        public long initialAvailableSpaceOnServer; // BYTES
        public long currentAvailableSpaceOnServer; // BYTES
        public String protocolVersion;
        public ArrayList<FileToBackup> filesToBackup;
    }
}
