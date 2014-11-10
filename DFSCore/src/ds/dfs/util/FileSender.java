package ds.dfs.util;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Files;

public class FileSender extends Thread {

	String file = null;
	Socket socket = null;
	ObjectInputStream objIn = null;
	ObjectOutputStream objOut = null;

	public FileSender(String f, Socket s, ObjectInputStream in,
			ObjectOutputStream out) {
		file = f;
		socket = s;
		objIn = in;
		objOut = out;
	}

	@Override
	public void run() {
		try {
			System.out.println("Yay!");
			File f = new File(file);
			byte[] content = Files.readAllBytes(f.toPath());
			objOut.writeObject(content);
			objOut.close();
			objIn.close();
			socket.close();
		} catch (IOException e) {
			System.out.println("IOException occurred: " + e.getMessage());
		}
	}

}
