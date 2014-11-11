package ds.mapreduce.JobTracker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;

import ds.dfs.dfsclient.DFSClient;
import ds.mapreduce.Common.Command;
import ds.mapreduce.Common.JobSubmission;
import ds.mapreduce.Common.MRMessage;
import ds.mapreduce.Common.TaskResult;
import ds.mapreduce.Common.TaskType;

public class JobTrackerRequestHandler extends Thread {
	private Socket client;
	private ObjectInputStream inStream = null;
	private ObjectOutputStream outStream = null;
	private JobTracker tracker = null;

	public JobTrackerRequestHandler(Socket c, JobTracker t, ObjectOutputStream o, 
			ObjectInputStream i) {
		client = c;
		tracker = t;
		outStream = o;
		inStream = i;
	}

	private ArrayList<InputSplit> computeSplits(String iPath) {
		// Simple splitter splitting every 100 lines.
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		int fileSize = 1000; // TODO get from NN
		int linesRemaining = fileSize;

		while (linesRemaining > 0) {
			InputSplit split;
			if (linesRemaining < 100) {
				split = new InputSplit(iPath, (fileSize - linesRemaining),
						linesRemaining);
				splits.add(split);
				break;
			}
			split = new InputSplit(iPath, (fileSize - linesRemaining), 100);
			linesRemaining -= 100;
		}
		return splits;
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
		tracker.addJob(jobId);
		DFSClient dfsClient = new DFSClient("127.0.0.1"); 
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		//ArrayList<String> parts = dfsClient.getFileParts(iPath);
		ArrayList<String> parts = new ArrayList<String>();
		for (String part : parts) {
			InputSplit split = new InputSplit(part, 0, 0);
			splits.add(split);
		}
		int reducers = 10; // TODO Make configurable

		for (InputSplit split : splits) {
			MapTask t = new MapTask(split, jobId, TaskState.PENDING,
					TaskType.MAP, jPath, "m" + mapTaskCount++, reducers, oPath);
			tracker.addMapTask(t);
		}

		// Add reduce tasks to queue as well. Get executed only after maps(how?)
		for (int i = 0; i < 10; i++) {
			ReduceTask r = new ReduceTask(i + 1, jobId, TaskState.PENDING,
					TaskType.REDUCE, jPath, "r" + (i + 1), reducers, oPath);
			tracker.addReduceTask(r);
		}
	}

	@Override
	public void run() {
		try {
			//outStream = new ObjectOutputStream(client.getOutputStream());
			//inStream = new ObjectInputStream(client.getInputStream());
			MRMessage msg = (MRMessage) inStream.readObject();
			if (msg.getCommand() == Command.SUBMIT) {
				System.out.println("Received a submit command");
				addJob((JobSubmission) msg.getPayload());
			}
			if (msg.getCommand() == Command.HEARTBEAT) {
				// Mark as healthy
				Boolean isIdle = (Boolean) msg.getPayload();
				if (isIdle) {
					Task task = tracker.assignTask(client.getInetAddress()
							.getHostName().toString());
					if (task != null) {
						MRMessage taskMsg = new MRMessage(Command.TASK, task);
						outStream.writeObject(taskMsg);
					}
				}

			}
			if (msg.getCommand() == Command.COMPLETE) {
				TaskResult result = (TaskResult) msg.getPayload();

				// If map task? Get locations of map output?
				// If reduce task?
			}
			client.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
