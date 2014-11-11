package ds.mapreduce.Common;

import java.util.ArrayList;
import java.util.HashMap;

public class MapOutputCollector {
	String outputPath; //DFS file path
	int numReduceTasks;	//how many files do we need to write into
	String jobId;
	int taskId;
	HashMap<Integer, ArrayList<String>> buffers;
	
	int getPartition(String key, int numReduceTasks) {
        return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
	
	public MapOutputCollector(String oPath, int nReducers, String jId, int tId){
		outputPath = oPath;
		numReduceTasks = nReducers;
		jobId = jId;
		taskId = tId;
		buffers = new HashMap<Integer, ArrayList<String>>();
	}
	
	public void writeToBuffers(String key, String value) {
		//Create buffers for each partition
		int partition = getPartition(key, numReduceTasks);
		if(!buffers.containsKey(partition))
			buffers.put(partition, new ArrayList<String>());
		buffers.get(partition).add(key + " " + value);
	}
	
	public void flush(){
		//DFS copy file to DFS
		
	}
	
}
