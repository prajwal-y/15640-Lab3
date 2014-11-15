package ds.dfs.namenode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import ds.dfs.comm.DFSMessage;
import ds.dfs.util.Command;
import ds.dfs.util.DataNodeMetadata;

public class DataNodeHandler extends Thread {

	private Socket socket = null;
	ObjectInputStream in = null;
	ObjectOutputStream out = null;

	public DataNodeHandler(Socket s, ObjectInputStream i, ObjectOutputStream o) {
		socket = s;
		in = i;
		out = o;
	}

	@Override
	public void run() {
		try {
			out.writeObject(new DFSMessage(Command.OK, ""));
			out.flush();
			//Send OK to DataNode so that it can continue to send message
			DFSMessage msg = (DFSMessage) in.readObject();
			if (msg.getCommand() == Command.REGISTER) {
				DataNodeMetadata metadata = (DataNodeMetadata)msg.getPayload();
				NameNode.addDataNode(metadata.dataNodeId, metadata);
				System.out.println("Data node registered: " + metadata.host);
			}
			socket.close();
		} catch (IOException e) {
			System.out.println("IOException occurred: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException occurred: " + e.getMessage());
		}
	}

}
