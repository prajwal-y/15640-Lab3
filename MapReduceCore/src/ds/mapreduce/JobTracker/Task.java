package ds.mapreduce.JobTracker;

import java.io.Serializable;

public interface Task extends Serializable{
	public String getJobId();	
	public TaskState getState();
	public void setState(TaskState t);
}
