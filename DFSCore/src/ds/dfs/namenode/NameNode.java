package ds.dfs.namenode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
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
	public static ArrayList<DFSFile> fileList = new ArrayList<DFSFile>();
	private static ServerSocket server = null;
	public static String DFS_ROOT;

	public static void addDataNode(String dataNodeId, DataNodeMetadata metadata) {
		dataNodes.put(dataNodeId, metadata);
	}

	public static void listDFSFiles() {

	}

	/**
	 * Returns all the split parts
	 * 
	 * @param path
	 * @return
	 */
	public static ArrayList<String> getFileParts(String path) {
		for (DFSFile file : fileList) {
			if (file.fileName.equals(path))
				return new ArrayList<String>(file.partitionLoc.keySet());
		}
		return new ArrayList<String>();
	}

	/**
	 * Delete temporarily stored files
	 */
	private void cleanTempFolder() {
		File file = new File(DFS_ROOT + "/tempFolder");
		String[] myFiles;
		if (file.isDirectory()) {
			myFiles = file.list();
			for (int i = 0; i < myFiles.length; i++) {
				File myFile = new File(file, myFiles[i]);
				myFile.delete();
			}
		}
	}

	/**
	 * Splits the file based on number of lines.
	 * 
	 * @param file
	 */
	public static void splitAndTransferFile(String file, String dfsFolder) {
		try {
			System.out.println(file);
			String fullFilePath = DFS_ROOT + "/" + dfsFolder + "/" + file;
			FileInputStream fstream = new FileInputStream(fullFilePath);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			ArrayList<String> lines = new ArrayList<String>();
			int count = 0, partitionCount = 1;
			HashMap<String, ArrayList<String>> partFiles = new HashMap<String, ArrayList<String>>();
			// Split the file based on SPLIT_SIZE
			while ((strLine = br.readLine()) != null) {
				lines.add(strLine);
				count++;
				if (count == Constants.SPLIT_SIZE) {
					File partFile = new File(DFS_ROOT + "/tempFolder/"
							+ dfsFolder + "/" + file + "/" + file + "_"
							+ partitionCount);
					final File parent_directory = partFile.getParentFile();
					if (null != parent_directory)
						parent_directory.mkdirs();
					FileWriter fw = new FileWriter(partFile);
					BufferedWriter out = new BufferedWriter(fw);
					for (String s : lines) {
						out.write(s + "\n");
					}
					out.close();
					sendFilesToDataNodes(partFile.getAbsolutePath(), dfsFolder
							+ "/" + file + "/" + partFile.getName());
					ArrayList<String> dataNodeList = new ArrayList<String>();
					for (String dn : dataNodes.keySet()) {
						dataNodeList.add(dataNodes.get(dn).host);
					}
					partFiles.put(
							dfsFolder + "/" + file + "/" + partFile.getName(),
							dataNodeList);
					count = 0;
					partitionCount++;
					lines.clear();
				}
			}
			br.close();
			if (count != 0) {
				File partFile = new File(DFS_ROOT + "/tempFolder/" + dfsFolder
						+ "/" + file + "/" + file + "_" + partitionCount);
				final File parent_directory = partFile.getParentFile();
				if (null != parent_directory)
					parent_directory.mkdirs();
				FileWriter fw = new FileWriter(partFile);
				BufferedWriter out = new BufferedWriter(fw);
				for (String s : lines) {
					out.write(s + "\n");
				}
				sendFilesToDataNodes(partFile.getAbsolutePath(), dfsFolder
						+ "/" + file + "/" + partFile.getName());
				ArrayList<String> dataNodeList = new ArrayList<String>();
				for (String dn : dataNodes.keySet()) {
					dataNodeList.add(dataNodes.get(dn).host);
				}
				partFiles.put(
						dfsFolder + "/" + file + "/" + partFile.getName(),
						dataNodeList);
				out.close();
			}
			DFSFile newDfsFile = new DFSFile(dfsFolder + "/" + file,
					partitionCount, partFiles);
			fileList.add(newDfsFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prints all the existing DataNodes
	 */
	public static void printDataNodes() {
		for (String s : dataNodes.keySet()) {
			System.out.println(s);
		}
	}

	/**
	 * Prints all the file in DFS
	 */
	public static void listFiles() {
		for (DFSFile file : fileList) {
			System.out.println(file.fileName);
		}
	}

	/**
	 * Sends the specified file to all the DataNodes (Replication)
	 * 
	 * @param file
	 * @param dataFileName
	 */
	public static void sendFilesToDataNodes(String file, String dataFileName) {
		for (String s : dataNodes.keySet()) {
			Socket socket;
			try {
				socket = new Socket(dataNodes.get(s).host,
						Constants.DATANODE_PORT);
				ObjectOutputStream out = new ObjectOutputStream(
						socket.getOutputStream());
				out.writeObject(new DFSMessage(Command.NAMENODE, ""));
				out.flush();
				ObjectInputStream in = new ObjectInputStream(
						socket.getInputStream());
				if (((DFSMessage) in.readObject()).getCommand() == Command.OK) {
					out.writeObject(new DFSMessage(Command.CREATE, dataFileName));
					out.flush();
					if (((DFSMessage) in.readObject()).getCommand() == Command.OK)
						// Initiate sending file
						new FileSender(file, socket, in, out).start();
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

	public static void main(String[] args) {
		DFS_ROOT = args[0];
		try {
			// Start the NameNode server
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
				// socket.close();
			}
		} catch (IOException e) {
			System.out.println("IOException occurred: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException occurred: "
					+ e.getMessage());
		}
	}

}
