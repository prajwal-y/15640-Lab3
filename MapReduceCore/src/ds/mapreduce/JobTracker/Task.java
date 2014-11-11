package ds.mapreduce.JobTracker;

import java.io.Serializable;

import ds.mapreduce.Common.TaskType;

public class Task implements Serializable{
	private TaskType type;
	private TaskState state;
	private String jobId;
	private String taskId;
	private String jarPath;
	
	public Task(TaskType t, TaskState s, String id, String jPath, String tId){
		type = t;
		state = s;
		jobId = id;
		jarPath = jPath;
		taskId = tId;
	}
	
	public String getJobId() {
		return jobId;
	}	
	public TaskState getState() {
		return state;
	}
	public void setState(TaskState t) {
		state = t;
	}
	
	public TaskType getTaskType() {
		return type;
	}
	
	public String getJarPath(){
		return jarPath;
	}
	
	public String getTaskId(){
		return taskId;
	}
}
