package ds.mapreduce.Common;

import ds.mapreduce.JobTracker.TaskState;

public class TaskResult {
	private TaskType type;
	private TaskState state;
	private String outputLocation;
	
	public TaskResult(TaskType t, TaskState s, String output){
		type = t;
		state = s;
		output = outputLocation;
	}
}
