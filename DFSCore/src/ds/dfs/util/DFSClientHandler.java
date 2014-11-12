package ds.dfs.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

import ds.dfs.comm.DFSMessage;
import ds.dfs.datanode.DataNode;
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
			// Send OK to DataNode so that it can continue to send message
			out.writeObject(new DFSMessage(Command.OK, ""));
			out.flush();
			DFSMessage msg = (DFSMessage) in.readObject();
			if (msg.getCommand() == Command.FILETODFS) {
				FileObject fo = (FileObject) msg.getPayload();
				String dfsFolder = fo.folderPathInDFS;
				String fileName = fo.file;
				System.out.println(fileName);
				String tempFilePath = NameNode.DFS_ROOT + "/" + dfsFolder + "/" + fileName;
				Thread thread = (Thread) new FileReceiver(tempFilePath, socket, out, in);
				thread.start();
				thread.join();
				if (fo.isSplittable)
					NameNode.splitAndTransferFile(fileName, dfsFolder);
				else {
					String[] fileList = tempFilePath.split("/");
					NameNode.sendFilesToDataNodes(tempFilePath, fileList[fileList.length - 1] + "/" + fileList[fileList.length - 1]);
				}
				// NameNode.addFileToDFS((String)msg.getPayload());
			} else if (msg.getCommand() == Command.GETFILEPARTS) {
				String fileName = (String) msg.getPayload();
				boolean found = false;
				System.out.println("File in getfileparts is: " + fileName);
				for (DFSFile file : NameNode.fileList) {
					System.out.println("Entered list: " + file.fileName);
					if (file.fileName.equals(fileName)) {
						found = true;
						System.out.println("File Found. Returning parts");
						out.writeObject(new DFSMessage(Command.OK, new ArrayList<String>(file.partitionLoc.keySet())));
						break;
					}
				}
				if (!found)
					out.writeObject(new DFSMessage(Command.OK, null));
			} else if (msg.getCommand() == Command.LISTFILES) {
				// TODO:
			} else if (msg.getCommand() == Command.GETFILEDATA) {
				String fileName = (String) msg.getPayload();
				System.out.println("Filename in GetFileData is: " + fileName);
				boolean found = false;
				for (DFSFile d : NameNode.fileList) {
					if (d.fileName.equals(fileName)) {
						found = true;
						System.out.println("Found the file!");
						out.writeObject(new DFSMessage(Command.FILEDATA, d));
						break;
					}
				}
				if (!found)
					out.writeObject(new DFSMessage(Command.FILEDATA, null));
			} else if (msg.getCommand() == Command.GETFILEPARTDATA) {
				String fileName = (String) msg.getPayload();
				System.out.println("Filename in GetFilePartData is: " + fileName);
				boolean found = false;
				for (DFSFile d : NameNode.fileList) {
					for (String s : d.partitionLoc.keySet()) {
						if (s.equals(fileName)) {
							found = true;
							System.out.println("Found the file!");
							out.writeObject(new DFSMessage(Command.FILEDATA, d.partitionLoc.get(fileName)));
							break;
						}
					}
					if(found)
						break;
				}
				if (!found)
					out.writeObject(new DFSMessage(Command.FILEDATA, null));
			} else if (msg.getCommand() == Command.FILETOLOCAL) {
				String file = (String) msg.getPayload();
				new FileSender(DataNode.DFS_ROOT + "/" + file, socket, in, out).start();
			}
		} catch (IOException e) {
			System.out.println("IOException occurred: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException occurred: " + e.getMessage());
		} catch (InterruptedException e) {
			System.out.println("ClassNotFoundException occurred: " + e.getMessage());
		}
	}

}
