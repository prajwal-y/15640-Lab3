package ds.dfs.dfsclient;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import ds.dfs.comm.DFSMessage;
import ds.dfs.util.Command;
import ds.dfs.util.Constants;
import ds.dfs.util.FileObject;
import ds.dfs.util.FileSender;

public class DFSClient {

	String nameNodeHost = null;
	
	public DFSClient(String host) {
		nameNodeHost = host;
	}
	
	public void copyToLocal(String file, String localFilePath) {
		
	}
	
	/**
	 * Copy a local file to DFS
	 * @param file
	 * @param DFSfilePath
	 * @param split
	 */
	public void copyToDFS(String file, String DFSfilePath, boolean split) {
		try {
			Socket client = new Socket(nameNodeHost, Constants.NAMENODE_PORT);
			ObjectOutputStream outStream = new ObjectOutputStream(
					client.getOutputStream());
			outStream.writeObject(new DFSMessage(Command.DFSCLIENT, ""));
			ObjectInputStream inStream = new ObjectInputStream(
					client.getInputStream());
			DFSMessage msg = (DFSMessage) inStream.readObject();
			if (msg.getCommand() == Command.OK) {
				String[] fileSplit = file.split("/");
				String filePath = DFSfilePath + "/" + fileSplit[fileSplit.length - 1];	
				FileObject fo = new FileObject(filePath, split);
				outStream.writeObject(new DFSMessage(Command.FILETODFS,
						fo));
				System.out.println(filePath);
				new FileSender(file, client, inStream, outStream).start();
			}
			else {
				System.out.println("Cannot copy file to DFS");
			}
			//client.close();
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException: " + e.getMessage());
		}
	}

}
