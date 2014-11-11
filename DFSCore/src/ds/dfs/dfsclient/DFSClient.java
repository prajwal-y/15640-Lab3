package ds.dfs.dfsclient;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import ds.dfs.comm.DFSMessage;
import ds.dfs.util.Command;
import ds.dfs.util.Constants;
import ds.dfs.util.DFSFile;
import ds.dfs.util.FileObject;
import ds.dfs.util.FileReceiver;
import ds.dfs.util.FileSender;

/**
 * 
 * This class is a utility class to access the DFS
 * @author pyadapad and rjupadhy
 *
 */
public class DFSClient {

	String nameNodeHost = null;

	public DFSClient(String host) {
		nameNodeHost = host;
	}

	/**
	 * Copies the file from DFS to local machine
	 */
	public void copyToLocal(String file, String DFSfilePath, String localFilePath) {
		try {
			Socket client = new Socket(nameNodeHost, Constants.NAMENODE_PORT);
			ObjectOutputStream outStream = new ObjectOutputStream(client.getOutputStream());
			outStream.writeObject(new DFSMessage(Command.DFSCLIENT, ""));
			ObjectInputStream inStream = new ObjectInputStream(client.getInputStream());
			DFSMessage msg = (DFSMessage) inStream.readObject();
			String[] dfsFolderSplit = DFSfilePath.split("DFS://");
			if (dfsFolderSplit.length > 1)
				DFSfilePath = DFSfilePath.split("DFS://")[1];
			else
				DFSfilePath = "";
			if (msg.getCommand() == Command.OK) {
				outStream.writeObject(new DFSMessage(Command.GETFILEDATA, DFSfilePath + "/" + file));
				System.out.println(file);
				DFSMessage dfsMsg = (DFSMessage) inStream.readObject();
				DFSFile dfsFile = (DFSFile) dfsMsg.getPayload();
				outStream.close();
				inStream.close();
				client.close();
				if (dfsFile == null)
					System.out.println("File not found in DFS");
				else {
					for (String partFile : dfsFile.partitionLoc.keySet()) {
						client = new Socket(dfsFile.partitionLoc.get(partFile).get(0), Constants.DATANODE_PORT);
						outStream = new ObjectOutputStream(client.getOutputStream());
						outStream.writeObject(new DFSMessage(Command.DFSCLIENT, ""));
						inStream = new ObjectInputStream(client.getInputStream());
						DFSMessage message = (DFSMessage) inStream.readObject();
						if (message.getCommand() == Command.OK) {
							outStream.writeObject(new DFSMessage(Command.FILETOLOCAL, partFile));
							System.out.println("Preparing to receive from DataNode");
							new FileReceiver(localFilePath + "/" + partFile, client, outStream, inStream).start();
						}
					}
				}
			} else {
				System.out.println("Cannot copy file from DFS");
			}
			// client.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("IOException: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("ClassNotFoundException: " + e.getMessage());
		}
	}

	/**
	 * Copy a local file to DFS
	 * 
	 * @param file
	 * @param DFSfilePath
	 * @param split
	 */
	public void copyToDFS(String file, String DFSfilePath, boolean split) {
		try {
			Socket client = new Socket(nameNodeHost, Constants.NAMENODE_PORT);
			ObjectOutputStream outStream = new ObjectOutputStream(client.getOutputStream());
			outStream.writeObject(new DFSMessage(Command.DFSCLIENT, ""));
			ObjectInputStream inStream = new ObjectInputStream(client.getInputStream());
			DFSMessage msg = (DFSMessage) inStream.readObject();
			String[] dfsFolderSplit = DFSfilePath.split("DFS://");
			if (dfsFolderSplit.length > 1)
				DFSfilePath = DFSfilePath.split("DFS://")[1];
			else
				DFSfilePath = "";
			if (msg.getCommand() == Command.OK) {
				String[] fileSplit = file.split("/");
				FileObject fo = new FileObject(fileSplit[fileSplit.length - 1], DFSfilePath, split);
				outStream.writeObject(new DFSMessage(Command.FILETODFS, fo));
				new FileSender(file, client, inStream, outStream).start();
			} else {
				System.out.println("Cannot copy file to DFS");
			}
			// client.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("IOException: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("ClassNotFoundException: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		DFSClient dfsClient = new DFSClient("127.0.0.1");
		//dfsClient.copyToDFS("C:/Users/Prajwal/Desktop/input.txt", "DFS://", true);
		dfsClient.copyToLocal("input.txt", "DFS://", "C:/Users/Prajwal/Desktop/DFS");
	}

}
