package ds.mapreduce.JobTracker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import ds.mapreduce.Common.Command;
import ds.mapreduce.Common.Constants;
import ds.mapreduce.Common.MRMessage;
import ds.mapreduce.Common.TaskResult;
import ds.mapreduce.Common.TaskType;

public class JobTracker {

	private static String nameNodeHost;
	private static ServerSocket server = null;
	//Hashmap of job ID to list of tasks (map/reduce)
	private static HashMap<String, ArrayList<MapTask>> mapQueue = null;
	private static HashMap<String, ArrayList<ReduceTask>> reduceQueue = null;
	//All jobs in the system currently
	private static ArrayList<String> jobQueue = null;
	//Which host is assigned which task
	private static HashMap<String, ArrayList<Task>> assignedTasks = null;
	//Hosts that have sent a HEARTBEAT in the past 5 seconds have a true value 
	private static HashMap<String, Boolean> aliveHosts = new HashMap<String, Boolean>();


	public String getNameNode(){
		return nameNodeHost;
	}
	
	private static void checkFailedNodes() {
		ScheduledExecutorService exec = Executors
				.newSingleThreadScheduledExecutor();
		exec.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				for (String host : aliveHosts.keySet()) {
					if (aliveHosts.get(host))
						aliveHosts.put(host, false);
					else {
						ArrayList<Task> assigned = assignedTasks.get(host);
						for (Task task : assigned){
							String jobId = task.getJobId();
							String taskId = task.getTaskId();
							ArrayList<MapTask> mapTasks = mapQueue.get(jobId);
							ArrayList<ReduceTask> reduceTasks = reduceQueue.get(jobId);
							for (MapTask mTask : mapTasks){
								if(mTask.getTaskId().equals(taskId))
									mTask.setState(TaskState.PENDING);
							}for (ReduceTask rTask : reduceTasks){
								if(rTask.getTaskId().equals(taskId))
									rTask.setState(TaskState.PENDING);
							}
							assigned.clear();
						}
					}

				}
			}
		}, 0, 15, TimeUnit.SECONDS);
	}
	
	public void setAlive(String hostName){
		if(!aliveHosts.containsKey(hostName))
			aliveHosts.put(hostName, false);
		aliveHosts.put(hostName, true);
	}

	public JobTracker(String nNodeHostname) {
		mapQueue = new HashMap<String, ArrayList<MapTask>>();
		reduceQueue = new HashMap<String, ArrayList<ReduceTask>>();
		jobQueue = new ArrayList<String>();
		assignedTasks = new HashMap<String, ArrayList<Task>>();
		checkFailedNodes();
	}

	/*
	 * Method to return task to idle task tracker based on locality of data if
	 * map task else just return a reduce task.
	 */
	public Task assignTask(String host) {
		if (jobQueue.isEmpty())
			return null;
		String currentJob = jobQueue.get(0);
		ArrayList<MapTask> mQueue = mapQueue.get(currentJob);
		ArrayList<ReduceTask> rQueue = reduceQueue.get(currentJob);
		boolean mapsOver = true;
		boolean reducesOver = true;
		if ((mQueue != null) && (!mQueue.isEmpty())) {
			for (Task task : mQueue) {
				if (task.getState() == TaskState.PENDING
						|| task.getState() == TaskState.RUNNING
						|| task.getState() == TaskState.FAILED)
					mapsOver = false;
				if (task.getState() == TaskState.PENDING
						|| task.getState() == TaskState.FAILED) {
					task.setState(TaskState.RUNNING);
					if (!assignedTasks.containsKey(host))
						assignedTasks.put(host, new ArrayList<Task>());
					assignedTasks.get(host).add(task);
					return task;
				}
			}
		}
		if ((rQueue != null) && (!rQueue.isEmpty()) && mapsOver) {
			for (Task task : rQueue) {
				if (task.getState() == TaskState.PENDING
						|| task.getState() == TaskState.RUNNING
						|| task.getState() == TaskState.FAILED)
					reducesOver = false;
				if (task.getState() == TaskState.PENDING
						|| task.getState() == TaskState.FAILED) {
					task.setState(TaskState.RUNNING);
					if (!assignedTasks.containsKey(host))
						assignedTasks.put(host, new ArrayList<Task>());
					assignedTasks.get(host).add(task);
					return task;
				}
			}
		}
		if (reducesOver)
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

	public void addJob(String jobId) {
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
				new JobTrackerRequestHandler(client, this).start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void markTask(TaskResult result, String hostName) {
		String jobId = result.getJobId();
		String taskId = result.getTaskId();
		TaskType type = result.getType();
		if (type == TaskType.MAP) {
			for (MapTask task : mapQueue.get(jobId)) {
				if (task.getTaskId().equals(taskId))
					task.setState(result.getState());
			}
		} else if (type == TaskType.REDUCE) {
			for (ReduceTask task : reduceQueue.get(jobId)) {
				if (task.getTaskId().equals(taskId))
					task.setState(result.getState());
			}
		}
		for (Task task : assignedTasks.get(hostName)) {
			if (task.getTaskId().equals(taskId)
					&& task.getJobId().equals(jobId)) {
				assignedTasks.get(hostName).remove(task);
				break;
			}
		}
	}
}
