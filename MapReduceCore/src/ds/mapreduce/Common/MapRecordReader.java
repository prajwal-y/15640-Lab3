package ds.mapreduce.Common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import ds.dfs.dfsclient.DFSClient;

public class MapRecordReader {
	String path;
	long currentRecord;
	BufferedReader fileReader;
	String nameNodeHost;
	
	
	private void openFile(){
		DFSClient dfsClient = new DFSClient(nameNodeHost);
		File file = null;
		try {
			file = dfsClient.openFile(path, true);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		try {
			fileReader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public MapRecordReader(String p, String nNodeHost){
		path = p;
		nameNodeHost = nNodeHost;
		openFile();
		currentRecord = 0;
	}
	
	public String nextRecord(){
		String record = null;
		try {
			record = fileReader.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		currentRecord++;
		return record;		
	}
}
