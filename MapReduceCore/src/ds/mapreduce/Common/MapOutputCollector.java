package ds.mapreduce.Common;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

import ds.dfs.dfsclient.DFSClient;

public class MapOutputCollector {
	String outputPath; // DFS file path
	Integer numReduceTasks; // how many files do we need to write into
	String jobId;
	String taskId;
	HashMap<Integer, ArrayList<String>> buffers;
	String nameNodeHost;

	int getPartition(String key, int numReduceTasks) {
		int partitionId = (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		/*PrintWriter out = null;
		try {
		     out = new PrintWriter(new BufferedWriter(new FileWriter("C:\\Users\\rohit\\Desktop\\mapOut.txt", true)));
		    out.println(key + " " + partitionId);
		} catch (IOException e) {
			e.printStackTrace();
		}
		out.close();*/
		return partitionId;
	}

	public MapOutputCollector(String oPath, int nReducers, String jId, String tId,
			String nNodeHost) {
		outputPath = oPath;
		numReduceTasks = nReducers;
		jobId = jId;
		taskId = tId;
		buffers = new HashMap<Integer, ArrayList<String>>();
		nameNodeHost = nNodeHost;
	}

	public void writeToBuffers(String key, String value) {
		// Create buffers for each partition
		int partition = getPartition(key, numReduceTasks);
		if (!buffers.containsKey(partition))
			buffers.put(partition, new ArrayList<String>());
		buffers.get(partition).add(key + " " + value + "\n");
	}

	public void flush(){
		DFSClient dfsClient = new DFSClient(nameNodeHost);
		for(Integer partitionId : buffers.keySet()){
			try {
				System.out.println("DFS://" + jobId + "/" + partitionId + " :: TaskID: " + taskId.toString() + " partitionID: " + partitionId);
				dfsClient.writeFile("DFS://" + jobId + "/" + partitionId, 
						taskId.toString(), buffers.get(partitionId));
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
