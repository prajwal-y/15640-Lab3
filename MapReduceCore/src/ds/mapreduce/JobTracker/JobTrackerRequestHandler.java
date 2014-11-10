package ds.mapreduce.JobTracker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;

import ds.mapreduce.Common.Command;
import ds.mapreduce.Common.JobSubmission;
import ds.mapreduce.Common.MRMessage;
import ds.mapreduce.Common.TaskType;

public class JobTrackerRequestHandler extends Thread{
	private Socket client;
	private ObjectInputStream inStream = null;
	private ObjectOutputStream outStream = null;
	private JobTracker tracker = null;
	
	public JobTrackerRequestHandler(Socket c, JobTracker t){
		client = c;
		tracker = t;
	}
	
	private ArrayList<InputSplit> computeSplits(String iPath){
		//Simple splitter splitting every 100 lines.
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		int fileSize = 1000; //TODO get from NN
		int linesRemaining = fileSize;
		
		while(linesRemaining > 0){
			InputSplit split;
			if(linesRemaining < 100){
				split = new InputSplit(iPath, (fileSize - linesRemaining), linesRemaining);
				splits.add(split);
				break;
			}
			split = new InputSplit(iPath, (fileSize - linesRemaining), 100);
			linesRemaining -=100;
		}		
		return splits;
	}
	
	private void addJob(JobSubmission job){
		String iPath = job.getInputPath();
		String oPath = job.getOutputPath();
		String jPath = job.getJarPath();
		String jobId = UUID.randomUUID().toString();
		tracker.addJob(jobId);
		ArrayList<InputSplit> splits = computeSplits(iPath);
		for (InputSplit split : splits){
			MapTask t = new MapTask(split, jobId, TaskState.PENDING);
			tracker.addMapTask(t);
		}		
		
		//Add reduce tasks to queue as well. Get executed only after maps(how?)
		int reducers = 10; //TODO Make configurable
		for (int i = 0; i < 10; i++){
			ReduceTask r = new ReduceTask(i+1, jobId, TaskState.PENDING);
			tracker.addReduceTask(r);
		}
	}
	
	@Override
	public void run() {
		try {
			inStream = new ObjectInputStream(client.getInputStream());
			outStream = new ObjectOutputStream(client.getOutputStream());
			MRMessage msg = (MRMessage) inStream.readObject();
			if(msg.getCommand() == Command.SUBMIT){
				System.out.println("Received a submit command");
				addJob((JobSubmission)msg.getPayload());				
			}
			if(msg.getCommand() == Command.HEARTBEAT){
				//Mark as healthy
				Boolean isIdle = (Boolean)msg.getPayload();
				if(isIdle){
					Task task = tracker.assignTask(client.getInetAddress().getHostName().
							toString());
					MRMessage taskMsg = new MRMessage(Command.TASK, task);
					outStream.writeObject(taskMsg);
				}
					
			}
			if(msg.getCommand() == Command.COMPLETE){
				//If map task? Get locations of map output?
				//If reduce task?
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
