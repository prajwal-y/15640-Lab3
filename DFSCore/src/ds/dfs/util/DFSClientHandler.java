package ds.dfs.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import ds.dfs.comm.DFSMessage;
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
			//Send OK to DataNode so that it can continue to send message
			out.writeObject(new DFSMessage(Command.OK, ""));
			out.flush();
			DFSMessage msg = (DFSMessage) in.readObject();
			if (msg.getCommand() == Command.FILETODFS) {
				FileObject fo = (FileObject) msg.getPayload();
				System.out.println((String)fo.file);
				String tempFilePath = NameNode.DFS_ROOT + ((String)fo.file).split("DFS://")[1];
				Thread thread = (Thread) new FileReceiver( NameNode.DFS_ROOT + ((String)fo.file).split("DFS://")[1], socket, out, in);
				thread.start();
				thread.join();
				if(fo.isSplittable)
					NameNode.splitFile(tempFilePath);
				else {
					String[] fileList = tempFilePath.split("/");
					NameNode.sendFilesToDataNodes(tempFilePath, fileList[fileList.length - 1] + "/" + fileList[fileList.length - 1]);
				}
				//NameNode.addFileToDFS((String)msg.getPayload());
			}
			else if(msg.getCommand() == Command.FILETONAMENODE) {
				//TODO:
			}
			else if(msg.getCommand() == Command.LISTFILES) {
				//TODO:
			}
			//socket.close();
		} catch (IOException e) {
			System.out.println("IOException occurred: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException occurred: " + e.getMessage());
		} catch (InterruptedException e) {
			System.out.println("ClassNotFoundException occurred: " + e.getMessage());
		} 
	}
	
}
