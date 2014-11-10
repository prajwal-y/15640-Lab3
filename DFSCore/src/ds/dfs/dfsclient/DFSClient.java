package ds.dfs.dfsclient;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import ds.dfs.comm.DFSMessage;
import ds.dfs.util.Command;
import ds.dfs.util.Constants;

public class DFSClient {

	public static void copyToDFS(String file, String DFSfilePath) {
		try {
			Socket client = new Socket("127.0.0.1", Constants.NAMENODE_PORT);
			ObjectOutputStream outStream = new ObjectOutputStream(
					client.getOutputStream());
			outStream.writeObject(new DFSMessage(Command.DFSCLIENT, ""));
			ObjectInputStream inStream = new ObjectInputStream(
					client.getInputStream());
			DFSMessage msg = (DFSMessage) inStream.readObject();
			if (msg.getCommand() == Command.OK) {
				outStream.writeObject(new DFSMessage(Command.FILETODFS,
						file));
			}
			else {
				System.out.println("Cannot copy file to DFS");
			}
			client.close();
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		// TODO: Accept commands
		String masterNodeHost = "127.0.0.1";
	}
}
