package server;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.sql.rowset.spi.SyncResolver;

import protocols.ProtocolMessage;
import constantValues.Values;

public class BackupChannelThread extends ChannelThread {
	
	/**The multicast socket this thread works on
	 * 
	 *  There is only one socket per type of channelThread, i.e the subclasses of ChannelThread;
	 *  That is why it wasn't extracted to  the superclass and also why it has to be static.
	 *  */
	private static MulticastSocket multicast_backup_socket;
    private static BackupChannelThread instance;
    private Thread backupDirectoryWatcher;
      
    private HashMap<String,ArrayList<Integer>> backedFiles;
       
	private BackupChannelThread(Server server) {
		
		this.setName("BackupChannelThread");
	    File backupDirectory = new File(Values.directory_to_backup_files);
	    backedFiles = new HashMap<String,ArrayList<Integer>>();
	    
	    if(backupDirectory.exists()) {
	        if(backupDirectory.isDirectory()) {
	            loadBackedUpFilesMap(backupDirectory);
	        } else {
	            System.out.println("There's a file with the name 'BACKUPS'. You have to delete it.");
	            System.exit(-1);
	        }
	    } else if(!backupDirectory.mkdir()) {
	        System.out.println("Error creating backups directory. You may not have write permission.");
            System.exit(-1);
	    }
	    
	    try {
            backupDirectoryWatcher = new WatchDir();
            backupDirectoryWatcher.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
	    setServer(server);
	}
	
	private void loadBackedUpFilesMap(File backupDirectory) {
	    File[] files = backupDirectory.listFiles();
	    
	    if(files.length > 0) {
	    	System.out.println("Loading BackedFiles json into memory");
	    }
	    
	    for(File file : files) {
	        if(file.getName().matches(Values.fileIdPattern)) {
	            ArrayList<Integer> chunkNumbers = new ArrayList<Integer>();
	            File[] chunks = file.listFiles();
	            for(File chunk : chunks) {
	                if(chunk.getName().matches(Values.chunkPattern)) {
	                    chunkNumbers.add(Integer.parseInt(chunk.getName().substring(6)));
	                } else {
	                    //System.out.println("This file shouldn't be here : "+file.getName());
	                }
	            }
	            if(chunks.length > 0) {
	                backedFiles.put(file.getName(), chunkNumbers);
	            }
	        } else {
	            //System.out.println("This file shouldn't be here : "+file.getName());
	        }
	    }
	}
	
	    
	public static BackupChannelThread getInstance(Server server) {
	    if(instance == null) {
	        instance = new BackupChannelThread(server);
	    }
	    return instance;
	}
	
	@Override
    public void run(){
		byte[] buffer = new byte[65000];
		DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
		while(true){
		    try{
		        multicast_backup_socket.receive(datagram);
		        if(!Server.fromThisMachine(datagram.getAddress())){
		            byte[] temp = new byte[datagram.getLength()];
		            System.arraycopy(datagram.getData(), 0, temp, 0, datagram.getLength());
		            incomingRequestsPool.execute(new RequestWorker(temp,datagram.getAddress()));
		        }
		    }catch(IOException e){
		        e.printStackTrace();
		    } 
		}
	}
	
	protected void processRequest(String request, InetAddress src) {

	    int endOfHeaderIndex;
	    if((endOfHeaderIndex = request.indexOf("\r\n\r\n")) != -1) { // find the end of the header
	        String requestHeader = request.substring(0, endOfHeaderIndex);
	        String headerPattern = "^PUTCHUNK 1.0 [a-z0-9]{64} [0-9]{1,6} [1-9]$";
	        System.out.println("------------------------Received backup request------------------------");
	        String[] fields = requestHeader.split(" ");
	        
	        if(requestHeader.matches(headerPattern)) {
	        	
	            try {
	                // waiting between 0 and 400 miliseconds before deciding if it will save the chunk
	                int delay = Server.rand.nextInt(Values.server_sending_packets_delay);
	                Thread.sleep(delay);
	            } catch (InterruptedException e1) {
	                e1.printStackTrace();
	            }
	            
	            synchronized (getServer().getReplicasRemovedFromOtherMachines()) {
	            	getServer().getReplicasRemovedFromOtherMachines().remove(fields[2]+":"+fields[3]); // this machine doesn't need to start backup subprotocol
	            }

	            boolean saveIt = false;
	            
	            synchronized (backedFiles) {
	                ArrayList<Integer> chunksBackedUp = backedFiles.get(fields[2]);
	                if(chunksBackedUp != null) {
	                    if(chunksBackedUp.contains(Integer.parseInt(fields[3]))) { // we have backed it up before
	                        sendStoredMessage(fields);
	                    } else {
	                        saveIt = true;
	                    }
	                } else { // we have no chunk from this file
	                	saveIt = true;
	                }

	                if(saveIt) {
	                	if(getServer().getAvailableSpaceOnServer() > Values.number_of_bytes_in_chunk) {
	                		ControlChannelThread cct = getServer().getControl_thread();
	                		if(cct.getNumberOfBackupsFromChunkNo(fields[2], Integer.parseInt(fields[3])) < Integer.parseInt(fields[4])) { // we save it

	                			sendStoredMessage(fields);
	                			String data = request.substring(endOfHeaderIndex+4);
	                			String fileSeparator = System.getProperty("file.separator");
	                			File directory = new File(Values.directory_to_backup_files+fileSeparator+fields[2]);
	                			File output = new File(Values.directory_to_backup_files+fileSeparator+fields[2]+fileSeparator+"chunk_"+fields[3]);

	                			try {
	                				if(!directory.mkdirs() && !directory.exists()) {
	                					System.out.println("ERROR CREATING FILE DIRECTORY.");
	                					//TODO SEND REMOVED NOTIFICATION!!!!
	                				} else {
	                					FileOutputStream fop = new FileOutputStream(output);
	                					fop.write(data.getBytes());
	                					fop.flush();
	                					fop.close();
	                					cct.incrementReplicationOfOtherChunk(fields[2], Integer.parseInt(fields[3]));
	                					getServer().getControl_thread().setChunksDesiredReplication(fields[2], Integer.parseInt(fields[3]), Integer.parseInt(fields[4])); // TODO TESTING

	                					try {
	                						backedFiles.get(fields[2]).add(new Integer(fields[3]));
	                					} catch (NullPointerException e) {
	                						ArrayList<Integer> chunks = new ArrayList<Integer>();
	                						chunks.add(new Integer(fields[3]));
	                						backedFiles.put(fields[2],chunks);
	                					}
	                				}
	                			} catch (IOException e) {
	                				e.printStackTrace();
	                			}
	                		} else {
	                			System.out.println("CHUNK ALREADY HAS DESIRED REPLICATION DEGREE - NO CHUNK BACKUP HERE");
	                		}
	                	} else {
	                		System.out.println("WE DON'T HAVE SPACE FOR THIS");
	                	}
	                } else {
	                    System.out.println("WE'VE SAVED THIS CHUNK BEFORE. JUST SENDING STORED MESSAGE.");
	                }
	            } 
	        } else {
	            System.out.println("Invalid header. Ignoring request");
	        }

	    }else{
	        System.out.println("No <CRLF><CRLF> detected. Ignoring request");
	    }
	}
	
	private void sendStoredMessage(String[] fields){
		try{
			String head = new String(Values.stored_chunk_control_message_identifier + " " + fields[1] + " " + fields[2] + " " + fields[3]);
			byte[] buf = ProtocolMessage.toBytes(head, null);
			DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_control_group_address, Values.multicast_control_group_port);
			ControlChannelThread.getMulticast_control_socket().send(packet);
			System.out.println(Thread.currentThread().getName() + " sent STORED message");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void send_REMOVED_messageForFile(String fileName) {
		
		int numberOfChunks = backedFiles.get(fileName).size();
		
		while(numberOfChunks-- > 0){
			send_REMOVED_messageForChunk(fileName, backedFiles.get(fileName).get(numberOfChunks-1)); //sends message in reverse order
		}
	
	}

	public void send_REMOVED_messageForChunk(String fileId, int chunkNum){
		
		try {
			String head = Values.diskSpace_reclaimed_control_message_identifier + " "
					+ Values.protocol_version + " "
					+ fileId + " "
					+ chunkNum;

			byte[] buf = ProtocolMessage.toBytes(head, null);
			DatagramPacket packet = new DatagramPacket(buf, buf.length, Values.multicast_control_group_address, Values.multicast_control_group_port);			
			
			int delay = Server.rand.nextInt(Values.backup_thread_response_delay);
			Thread.sleep(delay);
			ControlChannelThread.getMulticast_control_socket().send(packet);
			
			System.out.println("\n" + Thread.currentThread().getName() + " SENT REMOVED MESSAGE:"
									+ "\nFILE: " + fileId 
									+ "\nCHUNK: " + chunkNum
									+ "--------------------------------------------------------------");
			
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	private class WatchDir extends Thread {
        private final WatchService watcher;
        private final Map<WatchKey,Path> keys;
        private boolean trace = false;
        
        @SuppressWarnings("unchecked")
        <T> WatchEvent<T> cast(WatchEvent<?> event) {
            return (WatchEvent<T>)event;
        }
        
        public WatchDir() throws IOException {
            Path dir = Paths.get(Values.directory_to_backup_files);
            
            watcher = FileSystems.getDefault().newWatchService();
            keys = new HashMap<WatchKey,Path>();

            registerAll(dir);
            
            // enable trace after initial registration
            this.trace = true;
            setName("BackupDirectoryWatcherDaemonThread");
            setDaemon(true);
            
            System.out.println("Initialized BackupDirectoryWatcherDaemonThread");
        }
        
        /**
         * Register the given directory, and all its sub-directories, with the
         * WatchService.
         */
        private void registerAll(final Path start) throws IOException {
            // register directory and sub-directories
            Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException
                {
                    register(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        
        /**
         * Register the given directory with the WatchService
         */
        private void register(Path dir) throws IOException {
            WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
            if (trace) {
                Path prev = keys.get(key);
                if (prev == null) {
                    System.out.format("register: %s\n", dir);
                } else {
                    if (!dir.equals(prev)) {
                        System.out.format("update: %s -> %s\n", prev, dir);
                    }
                }
            }
            keys.put(key, dir);
        }

        public void run() {
            for (;;) {

            	// wait for key to be signaled
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException x) {
                    return;
                }

                Path dir = keys.get(key);
                if (dir == null) {
                    System.err.println("WatchKey not recognized!!");
                    continue;
                }
                
                for (WatchEvent<?> event: key.pollEvents()) {
                    WatchEvent.Kind kind = event.kind();

                    
                    // TBD - provide example of how OVERFLOW event is handled
                    if (kind == OVERFLOW) {
                        continue;
                    }

                    // Context for directory entry event is the file name of entry
                    WatchEvent<Path> ev = cast(event);
                    Path name = ev.context();
                    Path child = dir.resolve(name);

                    // print out event
                    //System.out.format("%s: %s\n", event.kind().name(), child);
                    
                    // if file folder or chunk is deleted, remove it from the backedFiles map
                    if(kind == ENTRY_DELETE) {
                        
                        String fileName = child.getFileName().toString();
                        
                        if(fileName.matches(Values.fileIdPattern)) {
                            synchronized (backedFiles) {
                               	send_REMOVED_messageForFile(fileName);
                                backedFiles.remove(fileName);
                            }
                        } else if(fileName.matches(Values.chunkPattern)) {
                            String fileId = child.getParent().getFileName().toString();
                            
                            if(fileId.matches(Values.fileIdPattern)) {
                                synchronized (backedFiles) {
                                	send_REMOVED_messageForChunk(fileId, Integer.parseInt(fileName.substring(6)));
                                    backedFiles.get(fileId).remove(Integer.parseInt(fileName.substring(6)));
                                    if(backedFiles.get(fileId).size() == 0) {
                                        backedFiles.remove(fileId);
                                    }
                                }
                            }
                        }
                    }

                    // if directory is created, and watching recursively, then
                    // register it and its sub-directories
                    if (kind == ENTRY_CREATE) {
                        if(child.getFileName().toString().matches(Values.fileIdPattern)) {
                            try {
                                if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
                                    registerAll(child);
                                }
                            } catch (IOException x) {
                                // ignore to keep sample readable
                            }
                        }
                    }
                }

                // reset key and remove from set if directory no longer accessible
                boolean valid = key.reset();
                if (!valid) {
                	System.out.println("DIRECTORY NOT ACCESSIBLE");
                    keys.remove(key);

                    // all directories are inaccessible
                    if (keys.isEmpty()) {
                        break;
                    }
                }
            }
        }
        
	}
	
	/**
	 * Init_socket.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void init_socket() throws IOException {
		
		multicast_backup_socket = new MulticastSocket(Values.multicast_backup_group_port);
		multicast_backup_socket.joinGroup(Values.multicast_backup_group_address);
		multicast_backup_socket.setTimeToLive(1);
	}

	public static MulticastSocket getMulticast_backup_socket() {
		return multicast_backup_socket;
	}
	public static void setMulticast_backup_socket(MulticastSocket multicast_backup_socket) {
		BackupChannelThread.multicast_backup_socket = multicast_backup_socket;
	}
}
