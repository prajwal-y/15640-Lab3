package ds.mapreduce.JobTracker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import ds.mapreduce.Common.Constants;
import ds.mapreduce.Common.TaskResult;
import ds.mapreduce.Common.TaskType;

public class JobTracker {

	private static ServerSocket server = null;
	private static HashMap<String, ArrayList<MapTask>> mapQueue = null;
	private static HashMap<String, ArrayList<ReduceTask>> reduceQueue = null;
	private static ArrayList<String> jobQueue = null;
	private static HashMap<String, ArrayList<Task>> assignedTasks = null;

	public JobTracker() {
		mapQueue = new HashMap<String, ArrayList<MapTask>>();
		reduceQueue = new HashMap<String, ArrayList<ReduceTask>>();
		jobQueue = new ArrayList<String>();
		assignedTasks = new HashMap<String, ArrayList<Task>>();
	}

	/* Method to return task to idle task tracker based on locality of data
	 * if map task else just return a reduce task.
	 */
	public Task assignTask(String host){
		if(jobQueue.isEmpty())
			return null;
		String currentJob = jobQueue.get(0);
		ArrayList<MapTask> mQueue = mapQueue.get(currentJob);
		ArrayList<ReduceTask> rQueue = reduceQueue.get(currentJob);
		boolean mapsOver = true;
		if((mQueue != null) && (!mQueue.isEmpty())){
			for (Task task : mQueue){
				if(task.getState() == TaskState.PENDING || task.getState() == TaskState.RUNNING
						|| task.getState() == TaskState.FAILED)
					mapsOver = false;
				if(task.getState() == TaskState.PENDING){
					task.setState(TaskState.RUNNING);
					if(!assignedTasks.containsKey(host))
						assignedTasks.put(host, new ArrayList<Task>());
					assignedTasks.get(host).add(task);
					return task;
				}					 
			}
		}
		if((rQueue != null) && (!rQueue.isEmpty())){
			for (Task task :rQueue){
				if(task.getState() == TaskState.PENDING || task.getState() == TaskState.RUNNING
						|| task.getState() == TaskState.FAILED)
					mapsOver = false;
				if(task.getState() == TaskState.PENDING){
					task.setState(TaskState.RUNNING);
					if(!assignedTasks.containsKey(host))
						assignedTasks.put(host, new ArrayList<Task>());
					assignedTasks.get(host).add(task);
					return task;
				}						 
			}
		}
		//Maps and reduces done, remove from job queue
		jobQueue.remove(0);
		return null;
	}
	
	public void addMapTask(MapTask t) {
		String jobId = t.getJobId();
		if (!mapQueue.containsKey(jobId)) {
			mapQueue.put(jobId, new ArrayList<MapTask>());
			mapQueue.get(jobId).add(t);
		} else {
			mapQueue.get(jobId).add(t);
		}
	}
	
	public void addJob(String jobId){
		jobQueue.add(jobId);
	}
	
	public void addReduceTask(ReduceTask t) {
		String jobId = t.getJobId();
		if (!reduceQueue.containsKey(jobId)) {
			reduceQueue.put(jobId, new ArrayList<ReduceTask>());
			reduceQueue.get(jobId).add(t);
		} else {
			reduceQueue.get(jobId).add(t);
		}
	}

	public void startEventLoop() {
		try {
			server = new ServerSocket(Constants.JOBTRACKER_PORT);
			while (true) {
				Socket client = server.accept();				
				new JobTrackerRequestHandler(
						client, this).start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void markTaskAsComplete(TaskResult result) {
		String jobId = result.getJobId();
		String taskId = result.getTaskId();
		TaskType type = result.getType();
		if (type == TaskType.MAP){
			for(MapTask task: mapQueue.get(jobId)){
				if(task.getTaskId().equals(taskId))
					task.setState(TaskState.COMPLETED);
			}
		}
		else if (type == TaskType.REDUCE){
			for(ReduceTask task: reduceQueue.get(jobId)){
				if(task.getTaskId().equals(taskId))
					task.setState(TaskState.COMPLETED);
			}
		}
	}
}
