package ds.mapreduce.JobTracker;

import java.io.Serializable;

public class InputSplit implements Serializable{
	private String filePath; //DFS path including part number
	//private String[] hosts;
	
	public InputSplit(String path){
		filePath = path;
	}
	
	public String getInputPath(){
		return filePath;
	}
}
