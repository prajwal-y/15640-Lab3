package ds.mapreduce.JobTracker;

public class InputSplit {
	private String filePath; //DFS path including part number
	private int start;
	private int length;
	//private String[] hosts;
	
	public InputSplit(String path, int s, int l){
		filePath = path;
		start = s;
		length = l;
		//hosts = h;
	}
	
	public String getInputPath(){
		return filePath;
	}
}
