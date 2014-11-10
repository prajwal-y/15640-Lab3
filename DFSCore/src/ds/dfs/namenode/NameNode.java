package ds.dfs.namenode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import ds.dfs.comm.DFSMessage;
import ds.dfs.util.Command;
import ds.dfs.util.Constants;
import ds.dfs.util.DFSClientHandler;
import ds.dfs.util.DFSFile;
import ds.dfs.util.DataNodeMetadata;
import ds.dfs.util.FileSender;

public class NameNode extends Thread {

	private static HashMap<String, DataNodeMetadata> dataNodes = new HashMap<String, DataNodeMetadata>();
	private static HashMap<String, DFSFile> fileList = new HashMap<String, DFSFile>();
	private static ServerSocket server = null;
	private static int dataNodeCount = 0;
	private static String dfsRoot;

	public static void addDataNode(String dataNodeId, DataNodeMetadata metadata) {
		dataNodes.put(dataNodeId, metadata);
	}

	public static void printDataNodes() {
		for (String s : dataNodes.keySet()) {
			System.out.println(s);
		}
	}

	public static void addFileToDFS(String file) {
		for (String s : dataNodes.keySet()) {
			Socket socket;
			try {
				socket = new Socket(dataNodes.get(s).host, Constants.DATANODE_PORT);
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(new DFSMessage(Command.NAMENODE, ""));
				out.flush();
				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				if(((DFSMessage)in.readObject()).getCommand() == Command.OK) {
					out.writeObject(new DFSMessage(Command.CREATE, file));
					out.flush();
					if(((DFSMessage)in.readObject()).getCommand() == Command.OK)
						new FileSender(dfsRoot + "\\" + file, socket, in, out).start();
				}
			} catch (UnknownHostException e) {
				System.out.println("UnknownHostException: " + e.getMessage());
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("IOException: " + e.getMessage());
			} catch (ClassNotFoundException e) {
				System.out.println("IOException: " + e.getMessage());
			}
		}
	}

	@Override
	public void run() {
		try {
			server = new ServerSocket(Constants.NAMENODE_PORT);
			System.out.println("NameNode server listening");
			while (true) {
				Socket socket = server.accept();
				ObjectInputStream in = new ObjectInputStream(
						socket.getInputStream());
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				DFSMessage message = (DFSMessage) in.readObject();
				if (message.getCommand() == Command.DATANODE) {
					new DataNodeHandler(socket, in, out).start();
				} else if (message.getCommand() == Command.DFSCLIENT) {
					new DFSClientHandler(socket, in, out).start();
				}
				//socket.close();
			}
		} catch (IOException e) {
			System.out.println("IOException occurred: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException occurred: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		//new NameNode().start();
		dfsRoot = args[0];
		try {
			server = new ServerSocket(Constants.NAMENODE_PORT);
			while (true) {
				System.out.println("NameNode server running");
				Socket socket = server.accept();
				ObjectInputStream in = new ObjectInputStream(
						socket.getInputStream());
				ObjectOutputStream out = new ObjectOutputStream(
						socket.getOutputStream());
				DFSMessage message = (DFSMessage) in.readObject();
				if (message.getCommand() == Command.DATANODE) {
					new DataNodeHandler(socket, in, out).start();
				} else if (message.getCommand() == Command.DFSCLIENT) {
					new DFSClientHandler(socket, in, out).start();
				}
				//socket.close();
			}
		} catch (IOException e) {
			System.out.println("IOException occurred: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException occurred: " + e.getMessage());
		}
	}

}
