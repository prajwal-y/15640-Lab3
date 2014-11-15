package ds.dfs.datanode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import ds.dfs.comm.DFSMessage;
import ds.dfs.util.Command;
import ds.dfs.util.FileReceiver;

public class NameNodeHandler extends Thread {
	
	Socket socket = null;
	String dfsRoot = null;
	ObjectInputStream in = null;
	ObjectOutputStream out = null;
	
	public NameNodeHandler(Socket s, String root, ObjectInputStream i, ObjectOutputStream o) {
		socket = s;
		dfsRoot = root;
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
			if (msg.getCommand() == Command.CREATE) {
				out.writeObject(new DFSMessage(Command.OK, ""));
				out.flush();
				FileReceiver receiver = new FileReceiver(dfsRoot + "/" + msg.getPayload(), socket, out, in);
				receiver.start();
			}
			else if (msg.getCommand() == Command.DELETE) {
				//TODO: Delete file
			}
			//socket.close();
		} catch (IOException e) {
			e.printStackTrace();
			//System.out.println("IOException occurred: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException occurred: " + e.getMessage());
		}
	}
}
