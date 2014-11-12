package ds.mapreduce.TaskTracker;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import ds.mapreduce.Common.Command;
import ds.mapreduce.Common.Constants;
import ds.mapreduce.Common.MRMessage;

public class TaskTracker {
	private static ServerSocket server = null;
	private static String jobTrackerHostName;
	private static Boolean isIdle;
	private static String nameNodeHost;
	
	private static void startHeartbeatThread(){
		ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
		exec.scheduleAtFixedRate(new Runnable() {
		  @Override
		  public void run() {
			  try {
				Socket client = new Socket(jobTrackerHostName, Constants.JOBTRACKER_PORT);
				ObjectOutputStream outStream = new ObjectOutputStream(
						client.getOutputStream());
				
				MRMessage msg = new MRMessage(Command.HEARTBEAT, isIdle);
				//System.out.println("Sent heartbeat to jobtracker");
				outStream.writeObject(msg);
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }
		}, 0, 5, TimeUnit.SECONDS);
	}
	
	public TaskTracker(String jTrackerHostName, String nNodeHost) {
		jobTrackerHostName = jTrackerHostName;
		nameNodeHost = nNodeHost;
		isIdle = true;
		startHeartbeatThread();
	}
	
	public void setIdle(Boolean idle){
		isIdle = idle;
	}
	
	public String getNameNode(){
		return nameNodeHost;
	}
	
	public void startEventLoop() {
		try {
			server = new ServerSocket(Constants.TASKTRACKER_PORT);
			while (true) {
				Socket client = server.accept();
				TaskTrackerRequestHandler handler = new TaskTrackerRequestHandler(
						client, this);
				new Thread(handler).start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
