package ds.dfs.datanode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.UUID;

import ds.dfs.comm.DFSMessage;
import ds.dfs.dfsclient.DFSClient;
import ds.dfs.util.Command;
import ds.dfs.util.Constants;
import ds.dfs.util.DFSClientHandler;
import ds.dfs.util.DFSFile;
import ds.dfs.util.DataNodeMetadata;

public class DataNode extends Thread {
	private static Socket client;
	private static ServerSocket server;
	private static String nameNodeHost;
	public static String DFS_ROOT;
	private static String dataNodeGuid = null;
	String id;

	private static boolean deleteFile(DFSFile file) {
		return false;
	}

	@Override
	public void run() {
		try {
			System.out.println("DataNode server listening");
			server = new ServerSocket(Constants.DATANODE_PORT);
			while (true) {
				Socket socket = server.accept();
				System.out.println("Received request");
				ObjectInputStream in = new ObjectInputStream(
						socket.getInputStream());
				DFSMessage message = (DFSMessage) in.readObject();
				if (message.getCommand() == Command.NAMENODE) {
					System.out.println("Name node is contacting");
					new NameNodeHandler(socket, DFS_ROOT, in, new ObjectOutputStream(socket.getOutputStream())).start();
				} else if (message.getCommand() == Command.DFSCLIENT) {
					System.out.println("DFSClient is contacting");
					new DFSClientHandler(socket, in, new ObjectOutputStream(socket.getOutputStream())).start();
				}
				//socket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("IOException occurred: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException occurred: "
					+ e.getMessage());
		}
	}

	public static void main(String[] args) {
		nameNodeHost = args[0];
		DFS_ROOT = args[1];
		dataNodeGuid = UUID.randomUUID().toString();
		try {
			client = new Socket(nameNodeHost, Constants.NAMENODE_PORT);
			ObjectOutputStream outStream = new ObjectOutputStream(client.getOutputStream());
			outStream.writeObject(new DFSMessage(Command.DATANODE, ""));
			ObjectInputStream inStream = new ObjectInputStream(client.getInputStream());
			DFSMessage msg = (DFSMessage) inStream.readObject();
			if (msg.getCommand() == Command.OK) {
				String hostName = InetAddress.getLocalHost().getHostName();
				DataNodeMetadata metadata = new DataNodeMetadata(hostName, dataNodeGuid, null);
				outStream.writeObject(new DFSMessage(Command.REGISTER, metadata));
			}
			client.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.out.println("UnknownHostException occurred: "
					+ e.getMessage());
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("IOException occurred: "
					+ e.getMessage());
			System.exit(0);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("ClassNotFoundException occurred: "
					+ e.getMessage());
			System.exit(0);
		}
		new DataNode().start();
	}
}
