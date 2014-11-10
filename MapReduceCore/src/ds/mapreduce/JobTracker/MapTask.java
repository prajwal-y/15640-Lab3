package ds.mapreduce.JobTracker;

import ds.mapreduce.Common.TaskType;

public class MapTask implements Task{
	private InputSplit split;
	private String jobId;
	private TaskState state;
	
	public MapTask(InputSplit s, String id, TaskState st){
		s = split;
		jobId = id;
		state = st;
	}
	
	public String getJobId(){
		return jobId;
	}
	
	public TaskState getState(){
		return state;
	}
	
	public void setState(TaskState t) {
		state = t;
	}
}
