package ds.mapreduce.JobTracker;

public class Job {
	private String inputPath; //on DFS
	private String outputPath; //on DFS
	private String jarPath;
	
	public Job(String iPath, String oPath, String jPath){
		inputPath = iPath;
		outputPath = oPath;
		jarPath = jPath;
	}

}
