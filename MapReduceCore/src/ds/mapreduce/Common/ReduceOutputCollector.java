package ds.mapreduce.Common;

import java.util.ArrayList;
import java.util.HashMap;

public class ReduceOutputCollector {
	String outputPath; //DFS file path
	ArrayList<String> buffer;
	
	int getPartition(String key, int numReduceTasks) {
        return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
	
	public ReduceOutputCollector(String oPath){
		outputPath = oPath;
		buffer = new ArrayList<String>();
	}
	
	public void writeToBuffers(String key, String value) {
		//Create buffers for each partition
		buffer.add(key + " " + value);
	}
	
	public void flush(){
		//DFS copy file to DFS
		
	}
	
}

