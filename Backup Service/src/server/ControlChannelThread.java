package server;

import java.io.IOException;
import java.net.*;
import protocols.Header;
import protocols.ProtocolMessage;
import constantValues.Values;



public class ControlChannelThread extends ChannelThread{
	
	/**The multicast socket this thread works on
	 * 
	 *  There is only one socket per type of channelThread, i.e the subclasses of ChannelThread;
	 *  That is why it wasn't extracted to the superclass and also why it has to be static.
	 *  */
	private static MulticastSocket multicast_control_socket;
	
	
	@Override
	public void run(){
		
		byte[] buffer = new byte[32];
		DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
		while(true){
			
			try{
				multicast_control_socket.receive(datagram);
			}catch(IOException e){
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			processRequest(datagram.getData().toString()); //it's okay to convert to string because on the control channel there is no chunk data 

		}
		
	}

	private void processRequest(String msg){
		
		System.out.println("\nControl Channel data:\n");
		System.out.println("Message received: " + msg);
	
		Header message = new Header(msg);
		
		switch(message.getMessageType()){
			
			case "STORED":
			{
				process_StoredMessage(message);
				break;
			}
			case "GETCHUNK":
			{
				process_GetChunkMessage(message);
				break;
			}
			case "DELETE":
			{
				process_DeleteMessage(message);
				break;
			}
			case "REMOVED":
			{
				process_RemovedMessage(message);
				break;
			}
			default:
			{
				System.out.println("Unrecognized message type");
				
				//TODO What happens here?!?! probably it's garbage, so maybe discard message?
				break;
			}
		}
		
		
	}

	private void process_RemovedMessage(Header message){
		// TODO Auto-generated method stub
		
	}

	private void process_DeleteMessage(Header message){
		// TODO Auto-generated method stub
		
	}

	private void process_GetChunkMessage(Header message){
		// TODO Auto-generated method stub
		
	}

	private void process_StoredMessage(Header message){
		// TODO Auto-generated method stub
		
	}
	
	

	/**
	 * Init_socket.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void init_socket() throws IOException{
		
		multicast_control_socket = new MulticastSocket(Values.multicast_control_group_port);
		multicast_control_socket.joinGroup(Values.multicast_control_group_address);
	}
	
	public static MulticastSocket getMulticast_control_socket(){
		return multicast_control_socket;
	}
	public static void setMulticast_control_socket(
			MulticastSocket multicast_control_socket){
		ControlChannelThread.multicast_control_socket = multicast_control_socket;
	}
	
}