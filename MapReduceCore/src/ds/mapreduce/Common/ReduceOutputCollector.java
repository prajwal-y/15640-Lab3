package ds.mapreduce.Common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import ds.dfs.dfsclient.DFSClient;

public class ReduceOutputCollector {
	String outputPath; //DFS file path
	ArrayList<String> buffer;
	String nameNodeHost;
	String taskId;
		
	public ReduceOutputCollector(String oPath, String nNameNodeHost, String tId){
		outputPath = oPath;
		buffer = new ArrayList<String>();
		nameNodeHost = nNameNodeHost;
		taskId = tId;
	}
	
	public void writeToBuffers(String key, String value) {
		//Create buffers for each partition
		buffer.add(key + " " + value);
	}
	
	public void flush(){
		//DFS copy file to DFS
		DFSClient dfsClient = new DFSClient(nameNodeHost);
		try {
			dfsClient.writeFile(outputPath, taskId, buffer);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

