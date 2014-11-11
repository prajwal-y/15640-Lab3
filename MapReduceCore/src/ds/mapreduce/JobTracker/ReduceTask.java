package ds.mapreduce.JobTracker;

import ds.mapreduce.Common.TaskType;

public class ReduceTask extends Task{
	private int partitionId;
	
	public ReduceTask(int pId, String jId, TaskState s, TaskType t, 
			String jPath, String tId, int nPart, String oPath){
		super(t, s, jId, jPath, tId, nPart, oPath);
		partitionId = pId;
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
	
	public int getPartitionId(){
		return partitionId;
	}
}
