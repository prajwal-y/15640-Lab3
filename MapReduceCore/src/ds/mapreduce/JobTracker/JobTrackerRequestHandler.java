package ds.mapreduce.JobTracker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;

import ds.dfs.dfsclient.DFSClient;
import ds.mapreduce.Common.ClusterStatus;
import ds.mapreduce.Common.Command;
import ds.mapreduce.Common.Constants;
import ds.mapreduce.Common.JobSubmission;
import ds.mapreduce.Common.MRMessage;
import ds.mapreduce.Common.TaskResult;
import ds.mapreduce.Common.TaskType;

public class JobTrackerRequestHandler extends Thread {
	private Socket client;
	private ObjectInputStream inStream = null;
	private ObjectOutputStream outStream = null;
	private JobTracker tracker = null;

	public JobTrackerRequestHandler(Socket c, JobTracker t) {
		client = c;
		tracker = t;
	}

	/**
	 * When user submits a job, divide the work into maps and reduces and add it
	 * to our task queues(map/reduce).
	 * 
	 * @param job
	 */
	private void addJob(JobSubmission job) {
		String iPath = job.getInputPath();
		String oPath = job.getOutputPath();
		String jPath = job.getJarPath();
		String jobId = UUID.randomUUID().toString();
		int mapTaskCount = 1;
		DFSClient dfsClient = new DFSClient(tracker.getNameNode());
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		ArrayList<String> parts = dfsClient.getFileParts(iPath);
		for (String part : parts) {
			InputSplit split = new InputSplit("DFS:/" + part);
			splits.add(split);
		}
		int reducers = 10; // TODO Make configurable

		for (InputSplit split : splits) {
			MapTask t = new MapTask(split, jobId, TaskState.PENDING,
					TaskType.MAP, jPath, "m" + mapTaskCount++, reducers, oPath);
			tracker.addMapTask(t);
		}

		// Add reduce tasks to queue as well. Get executed only after maps
		for (int i = 0; i < 10; i++) {
			ReduceTask r = new ReduceTask(i, jobId, TaskState.PENDING,
					TaskType.REDUCE, jPath, "r" + i, reducers, oPath);
			tracker.addReduceTask(r);
		}
		tracker.addJob(jobId);
	}

	@Override
	public void run() {
		try {
			outStream = new ObjectOutputStream(client.getOutputStream());
			inStream = new ObjectInputStream(client.getInputStream());
			MRMessage msg = (MRMessage) inStream.readObject();
			if (msg.getCommand() == Command.SUBMIT) {
				outStream.writeObject(new MRMessage(Command.HEARTBEAT, ""));
				System.out.println("Received a submit command");
				addJob((JobSubmission) msg.getPayload());
			}
			if (msg.getCommand() == Command.HEARTBEAT) {
				String hostname = client.getInetAddress().getHostName()
						.toString();
				tracker.setAlive(hostname);
				Boolean isIdle = (Boolean) msg.getPayload();
				// System.out.println("Received heartbeat from jobtrcker from" +
				// hostname);
				if (isIdle) {
					Task task = tracker.assignTask(hostname);
					if (task != null) {
						Socket tasktrackerSocket = new Socket(hostname,
								Constants.TASKTRACKER_PORT);
						ObjectOutputStream taskOutStream = new ObjectOutputStream(
								tasktrackerSocket.getOutputStream());
						MRMessage taskMsg = new MRMessage(Command.TASK, task);
						taskOutStream.writeObject(taskMsg);
						// tasktrackerSocket.close();
					}
				}
			}
			if (msg.getCommand() == Command.JOBSTATUS) {
				String hostname = client.getInetAddress().getHostName()
						.toString();
				TaskResult result = (TaskResult) msg.getPayload();
				tracker.markTask(result, hostname);
			}
			if (msg.getCommand() == Command.LISTJOBS) {
				ClusterStatus status = tracker.getClusterStatus();
				MRMessage statusMsg = new MRMessage(Command.LISTJOBS, status);
				outStream.writeObject(statusMsg);
			}
			// client.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

}
