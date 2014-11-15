package ds.dfs.util;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Files;

public class FileReceiver extends Thread {
	
	private String fileName = null;
	private Socket socket = null;
	ObjectOutputStream out;
	ObjectInputStream in;

	public FileReceiver(String fName, Socket s, ObjectOutputStream o, ObjectInputStream i) {
		fileName = fName;
		socket = s;
		out = o;
		in = i;
	}
	
	@Override
	public void run() {
		try {
			System.out.println("Trying to write to: " + fileName + " in FileReceiver");
			File f = new File(fileName);
			System.out.println(fileName);
			final File parent_directory = f.getParentFile();
			if (null != parent_directory)
				parent_directory.mkdirs();
			//f.createNewFile();
			byte[] bytes = (byte[])in.readObject();
			Files.write(f.toPath(), bytes);
			//socket.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("IOException occurred: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("ClassNotFoundException occurred: " + e.getMessage());
		}
	}
}
