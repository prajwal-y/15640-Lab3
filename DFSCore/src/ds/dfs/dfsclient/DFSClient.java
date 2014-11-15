package ds.dfs.dfsclient;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

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
 * 
 * @author pyadapad and rjupadhy
 *
 */
public class DFSClient {

	String nameNodeHost = null;

	public DFSClient(String host) {
		nameNodeHost = host;
	}

	/**
	 * Returns all parts of a given file in DFS
	 * 
	 * @param dfsPath
	 * @return
	 */
	public ArrayList<String> getFileParts(String dfsPath) {
		ArrayList<String> fileParts = new ArrayList<String>();
		try {
			System.out.println("GetFileParts: " + dfsPath);
			Socket client = new Socket(nameNodeHost, Constants.NAMENODE_PORT);
			ObjectOutputStream outStream = new ObjectOutputStream(client.getOutputStream());
			ObjectInputStream inStream = new ObjectInputStream(client.getInputStream());
			outStream.writeObject(new DFSMessage(Command.DFSCLIENT, ""));
			outStream.flush();
			DFSMessage msg = (DFSMessage) inStream.readObject();
			dfsPath = dfsPath.split("DFS://")[1];
			if (!dfsPath.contains("/"))
				dfsPath = "/" + dfsPath;
			if (msg.getCommand() == Command.OK) {
				System.out.println("Got OK from namenode");
				outStream.writeObject(new DFSMessage(Command.GETFILEPARTS, dfsPath));
				outStream.flush();
				DFSMessage message = (DFSMessage) inStream.readObject();
				System.out.println("Here its screwed");
				if (message != null) {
					for (String s : (ArrayList<String>) message.getPayload()) {
						fileParts.add(s);
					}
				}
			}
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("IOException: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("ClassNotFoundException: " + e.getMessage());
		}
		System.out.println("GetFileParts ended: " + dfsPath);
		return fileParts;
	}

	/**
	 * Returns a list of all files in a given DFS directory
	 * 
	 * @param dfsDir
	 * @return
	 * @throws IOException
	 * @throws UnknownHostException
	 * @throws ClassNotFoundException
	 */
	public ArrayList<String> listFilesinDirectory(String dfsDir) throws UnknownHostException, IOException, ClassNotFoundException {
		ArrayList<String> fileList = new ArrayList<String>();
		Socket client = new Socket(nameNodeHost, Constants.NAMENODE_PORT);
		ObjectOutputStream outStream = new ObjectOutputStream(client.getOutputStream());
		outStream.writeObject(new DFSMessage(Command.DFSCLIENT, ""));
		ObjectInputStream inStream = new ObjectInputStream(client.getInputStream());
		DFSMessage msg = (DFSMessage) inStream.readObject();
		String[] dfsDirSplit = dfsDir.split("DFS://");
		if (dfsDirSplit.length > 1)
			dfsDir = dfsDir.split("DFS://")[1];
		else
			dfsDir = "/";
		if (msg.getCommand() == Command.OK) {
			outStream.writeObject(new DFSMessage(Command.GETFILELIST, dfsDir));
			DFSMessage message = (DFSMessage) inStream.readObject();
			if (message.getPayload() != null) {
				for (String s : (ArrayList<String>) message.getPayload()) {
					fileList.add(s);
				}
			}
		}
		client.close();
		return fileList;
	}

	/**
	 * Reads a file from DFS
	 * 
	 * @param dfsFile
	 * @return
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public File openFile(String dfsFile, boolean isPart) throws ClassNotFoundException, IOException, InterruptedException {
		String[] split = dfsFile.split("DFS://");
		String localFile = this.copyToLocal(split[1], "DFS://", Constants.TEMP_FOLDER, isPart);
		File file = new File(localFile);
		return file;
	}

	/**
	 * Writes a file to DFS
	 * 
	 * @param dfsFolder
	 * @param fileName
	 * @param buffer
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException 
	 */
	public void writeFile(String dfsFolder, String fileName, ArrayList<String> buffer) throws IOException, ClassNotFoundException, InterruptedException {
		File tempFile = null;
		String tempFilePath = Constants.TEMP_FOLDER + fileName;
		tempFile = new File(tempFilePath);
		final File parent_directory = tempFile.getParentFile();
		if (null != parent_directory)
			parent_directory.mkdirs();
		FileWriter writer = new FileWriter(tempFile);
		for (String str : buffer) {
			writer.write(str);
		}
		writer.close();
		this.copyToDFS(tempFilePath, dfsFolder, false);
	}

	/**
	 * Copies the file from DFS to local machine
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public String copyToLocal(String file, String DFSfilePath, String localFilePath, boolean part) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("copyToLocal: " + file);
		String localFile = "";
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
			if (part) // Get Part file
				outStream.writeObject(new DFSMessage(Command.GETFILEPARTDATA, DFSfilePath + "/" + file));
			else {
				// Get full file (All parts)
				String fileName = null;
				  if(DFSfilePath.equals("")) 
					  fileName = file; 
				  else 
					  fileName = DFSfilePath + "/" + file;
				 
				outStream.writeObject(new DFSMessage(Command.GETFILEDATA, fileName));
			}
			System.out.println(file);
			DFSMessage dfsMsg = (DFSMessage) inStream.readObject();
			if (part) {
				ArrayList<String> partFileDataNodes = (ArrayList<String>) dfsMsg.getPayload();
				if (partFileDataNodes == null)
					System.out.println("File not found in DFS");
				else {
					client = new Socket(partFileDataNodes.get(0), Constants.DATANODE_PORT);
					outStream = new ObjectOutputStream(client.getOutputStream());
					outStream.writeObject(new DFSMessage(Command.DFSCLIENT, ""));
					inStream = new ObjectInputStream(client.getInputStream());
					DFSMessage message = (DFSMessage) inStream.readObject();
					if (message.getCommand() == Command.OK) {
						outStream.writeObject(new DFSMessage(Command.FILETOLOCAL, file));
						System.out.println("Preparing to receive from DataNode");
						Thread t = new FileReceiver(localFilePath + "/" + file, client, outStream, inStream);
						t.start();
						t.join();
						localFile = localFilePath + "/" + file;
					}
				}
			} else {
				DFSFile dfsFile = (DFSFile) dfsMsg.getPayload();
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
							Thread t = new FileReceiver(localFilePath + "/" + partFile, client, outStream, inStream);
							t.start();
							t.join();
							localFile = localFilePath + "/" + partFile;
						}
					}
				}
			}
			outStream.close();
			inStream.close();
			client.close();
		}
		System.out.println("copyToLocal ended: " + file);
		return localFile;
	}

	/**
	 * Copy a local file to DFS
	 * 
	 * @param file
	 * @param DFSfilePath
	 * @param split
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException 
	 */
	public void copyToDFS(String file, String DFSfilePath, boolean split) throws IOException, ClassNotFoundException, InterruptedException {
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
			Thread t = new FileSender(file, client, inStream, outStream);
			t.start();
			t.join();
			DFSMessage m = (DFSMessage)inStream.readObject();
			if(m.getCommand() == Command.OK) {
				System.out.println("File copied to DFS");
			}
		} else {
			System.out.println("Cannot copy file to DFS");
		}
		// client.close();
	}

	public static void main(String[] args) {
		DFSClient dfsClient = new DFSClient("127.0.0.1");
		// dfsClient.copyToDFS("C:/Users/Prajwal/Desktop/input.txt", "DFS://",
		// true);
		// dfsClient.openFile("input.txt/input.txt_1");

		 ArrayList<String> parts;
		try {
			parts = dfsClient.listFilesinDirectory("DFS://4379e17f-6f9d-4bc2-b430-c5f12a1d3f6b/0");
			 for(String p : parts)
				 System.out.println("Resulted list: " + p);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		 try {
			dfsClient.copyToLocal("4379e17f-6f9d-4bc2-b430-c5f12a1d3f6b/0/m1", "DFS://", "C:/Users/rohit/Desktop/DFS", false);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
