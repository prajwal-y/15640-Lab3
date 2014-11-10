package ds.mapreduce.JobTracker;

public class ReduceTask implements Task{
	private int partitionId;
	private String jobId;
	private TaskState state;
	
	public ReduceTask(int id, String jId, TaskState s){
		partitionId = id;
		jobId = jId;
		state = s;
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
