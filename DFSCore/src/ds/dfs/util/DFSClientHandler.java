package ds.dfs.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import ds.dfs.comm.DFSMessage;
import ds.dfs.namenode.NameNode;

public class DFSClientHandler extends Thread {

	Socket socket = null;
	ObjectInputStream in = null;
	ObjectOutputStream out = null;
	
	public DFSClientHandler(Socket s, ObjectInputStream i, ObjectOutputStream o) {
		socket = s;
		in = i;
		out = o;
	}
	
	@Override
	public void run() {
		try {
			//Send OK to DataNode so that it can continue to send message
			out.writeObject(new DFSMessage(Command.OK, ""));
			out.flush();
			DFSMessage msg = (DFSMessage) in.readObject();
			if (msg.getCommand() == Command.FILETODFS) {
				//NameNode.printDataNodes();
				System.out.println((String)msg.getPayload());
				String tempFilePath = NameNode.dfsRoot + ((String)msg.getPayload()).split("DFS://")[1];
				Thread thread = (Thread) new FileReceiver( NameNode.dfsRoot + ((String)msg.getPayload()).split("DFS://")[1], socket, out, in);
				thread.start();
				thread.join();
				NameNode.splitFile(tempFilePath);
				//NameNode.addFileToDFS((String)msg.getPayload());
			}
			else if(msg.getCommand() == Command.FILETONAMENODE) {
				NameNode.getFileToSelf();
			}
			else if(msg.getCommand() == Command.LISTFILES) {
				//TODO:
			}
			//socket.close();
		} catch (IOException e) {
			System.out.println("IOException occurred: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException occurred: " + e.getMessage());
		} catch (InterruptedException e) {
			System.out.println("ClassNotFoundException occurred: " + e.getMessage());
		} 
	}
	
}
