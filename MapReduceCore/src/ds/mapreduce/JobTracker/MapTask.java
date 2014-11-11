package ds.mapreduce.JobTracker;

import ds.mapreduce.Common.TaskType;

public class MapTask extends Task{
	private InputSplit split;
	
	public MapTask(InputSplit s, String id, TaskState st, TaskType t, 
			String jPath, String tId, int nPart, String oPath){
		super(t, st, id, jPath, tId, nPart, oPath);
		split = s;
	}
	
	public String getInputPath(){
		return split.getInputPath();
	}
	
	public String getJobId(){
		return super.getJobId();
	}
	
	public TaskState getState(){
		return super.getState();
	}
	
	public void setState(TaskState t) {
		super.setState(t);
	}
	
	public TaskType getTaskType(){
		return super.getTaskType();
	}
	
	public String getJarPath(){
		return super.getJarPath();
	}
}
