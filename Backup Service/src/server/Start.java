package server;

import java.io.IOException;
import java.net.UnknownHostException;
import constantValues.*;
import java.nio.*;

public class Start{
    
    private static void usage() {
        System.err.println("usage: java Start <MP_IP_ADDRESS> <MC_PORT> <MDB_IP_ADDRESS> <MDB_PORT> <MDR_IP_ADDRESS> <MDR_PORT>");
        System.exit(-1);
    }

	public static void main(String[] args){

	    if(args.length != 6) {
	        usage();
	    }
	    
		/*The first 6 arguments are MC, MDB and MDR, their IP address followed by port number */	
		try{
			Values.setMulticast_control_group_address(args[0]);
			Values.setMulticast_control_group_port(Integer.parseInt(args[1]));
			Values.setMulticast_backup_group_address(args[2]);
			Values.setMulticast_backup_group_port(Integer.parseInt(args[3]));
			Values.setMulticast_restore_group_address(args[4]);
			Values.setMulticast_restore_group_port(Integer.parseInt(args[5]));

			Server.initialize_sockets();

		}catch (UnknownHostException e){
			
			e.printStackTrace();
			System.out.println("A host could not be resolved, try again");
			System.exit(-1);
			
		}catch (IOException e){
			
			e.printStackTrace();
			System.out.println("Error when initializing socket, try again");
			System.exit(-1);
			
		}catch (ArrayIndexOutOfBoundsException e){

			e.printStackTrace();
			System.out.println("Missing arguments, try again");
			System.exit(-1);	
		}
		
		Server.getInstance().mainLoop();
	}
}
