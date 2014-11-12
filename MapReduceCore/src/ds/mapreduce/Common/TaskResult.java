package ds.mapreduce.Common;

import ds.mapreduce.JobTracker.TaskState;

public class TaskResult {
	private TaskType type;
	private TaskState state;
	private String jobId;
	private String taskId;
	
	public TaskType getType() {
		return type;
	}

	public TaskState getState() {
		return state;
	}

	public String getJobId() {
		return jobId;
	}

	public String getTaskId() {
		return taskId;
	}

	public TaskResult(TaskType t, TaskState s, String jId, String tId){
		type = t;
		state = s;
		jobId = jId;
		taskId = tId;
	}
}
