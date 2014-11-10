package ds.mapreduce.TaskTracker;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import ds.mapreduce.JobTracker.JobTracker;

public class TaskTrackerRequestHandler extends Thread{
	private Socket client;
	private ObjectInputStream inStream = null;
	private ObjectOutputStream outStream = null;
	private TaskTracker tracker = null;
	
	public TaskTrackerRequestHandler(Socket c, TaskTracker t){
		client = c;
		tracker = t;
	}
}
