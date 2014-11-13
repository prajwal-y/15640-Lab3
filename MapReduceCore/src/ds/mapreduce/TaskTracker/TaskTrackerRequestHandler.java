package ds.mapreduce.TaskTracker;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import ds.dfs.dfsclient.DFSClient;
import ds.mapreduce.Common.Command;
import ds.mapreduce.Common.Constants;
import ds.mapreduce.Common.MRMessage;
import ds.mapreduce.Common.MapOutputCollector;
import ds.mapreduce.Common.MapRecordReader;
import ds.mapreduce.Common.Mapper;
import ds.mapreduce.Common.ReduceOutputCollector;
import ds.mapreduce.Common.ReduceRecordReader;
import ds.mapreduce.Common.Reducer;
import ds.mapreduce.Common.TaskResult;
import ds.mapreduce.Common.TaskType;
import ds.mapreduce.JobTracker.MapTask;
import ds.mapreduce.JobTracker.ReduceTask;
import ds.mapreduce.JobTracker.Task;
import ds.mapreduce.JobTracker.TaskState;

public class TaskTrackerRequestHandler extends Thread {
	private Socket client;
	private ObjectInputStream inStream = null;
	private ObjectOutputStream outStream = null;
	private TaskTracker tracker;
	private Task currentTask;

	public TaskTrackerRequestHandler(Socket c, TaskTracker t) {
		client = c;
		tracker = t;
	}
	
	private void sendTaskResult(TaskState state){
		try {
			Socket client = new Socket(tracker.getJobTracker(), Constants.JOBTRACKER_PORT);
			ObjectOutputStream outStream = new ObjectOutputStream(
					client.getOutputStream());
			TaskResult result = new TaskResult(currentTask.getTaskType(), state, 
					currentTask.getJobId(), currentTask.getTaskId());
			MRMessage msg = new MRMessage(Command.JOBSTATUS, result);
			outStream.writeObject(msg);
			//outStream.close();
			//client.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void executeTask(Task t) {
		tracker.setIdle(false);
		currentTask = t;
		DFSClient dfsClient = new DFSClient(tracker.getNameNode());
		File jarFile = null;
		try {
			jarFile = dfsClient.openFile(t.getJarPath(), false);
		} catch (ClassNotFoundException e2) {
			e2.printStackTrace();
		} catch (IOException e2) {
			e2.printStackTrace();
		} catch (InterruptedException e2) {
			e2.printStackTrace();
		}
		String path = jarFile.getAbsolutePath();
		JarFile jar;		
		try {
			jar = new JarFile(path);
			Enumeration e = jar.entries();

			URL[] urls = { new URL("jar:file:" + path + "!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

			while (e.hasMoreElements()) {
				JarEntry je = (JarEntry) e.nextElement();
				if (je.isDirectory() || !je.getName().endsWith(".class")) {
					continue;
				}
				// -6 because of .class
				String className = je.getName().substring(0,
						je.getName().length() - 6);
				className = className.replace('/', '.');
				Class c = cl.loadClass(className);
				// if(Mapper.class.isAssignableFrom(c) ||
				// Reducer.class.isAssignableFrom(c)){
				if (t.getTaskType() == TaskType.MAP
						&& Mapper.class.isAssignableFrom(c)) {
					MapRecordReader mReader = new MapRecordReader(
							((MapTask) t).getInputPath(), tracker.getNameNode());
					MapOutputCollector mCollector = new MapOutputCollector(
							t.getOutputPath(), 10, t.getJobId(), t.getTaskId(),
							tracker.getNameNode());
					Mapper mapper = (Mapper) c.getConstructor(
							MapRecordReader.class, MapOutputCollector.class)
							.newInstance(mReader, mCollector);
					Thread thread = new Thread(mapper);
					Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
					    public void uncaughtException(Thread th, Throwable ex) {
					        sendTaskResult(TaskState.FAILED);
					        tracker.setIdle(true);
					        return;
					    }
					};
					thread.start();
					thread.join();
					thread.setUncaughtExceptionHandler(h);
					sendTaskResult(TaskState.COMPLETED);
					tracker.setIdle(true);
					return;
				} else if (t.getTaskType() == TaskType.REDUCE
						&& Reducer.class.isAssignableFrom(c)) {
					ReduceRecordReader rReader = new ReduceRecordReader(
							((ReduceTask) t).getPartitionId(), t.getJobId(),
							tracker.getNameNode());
					ReduceOutputCollector rCollector = new ReduceOutputCollector(
							t.getOutputPath(), tracker.getNameNode(),
							t.getTaskId());
					Reducer reducer = (Reducer) c.getConstructor(
							ReduceRecordReader.class,
							ReduceOutputCollector.class).newInstance(rReader,
							rCollector);
					Thread thread = new Thread(reducer);
					Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
					    public void uncaughtException(Thread th, Throwable ex) {
					        sendTaskResult(TaskState.FAILED);
					        tracker.setIdle(true);
					        return;
					    }
					};
					thread.start();
					thread.join();
					thread.setUncaughtExceptionHandler(h);
					sendTaskResult(TaskState.COMPLETED);
					tracker.setIdle(true);
					return;
				}
			}
			// }
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (InstantiationException e1) {
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			e1.printStackTrace();
		} catch (NoSuchMethodException e1) {
			e1.printStackTrace();
		} catch (SecurityException e1) {
			e1.printStackTrace();
		} catch (IllegalArgumentException e1) {
			e1.printStackTrace();
		} catch (InvocationTargetException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

	}

	@Override
	public void run() {
		try {
			inStream = new ObjectInputStream(client.getInputStream());
			outStream = new ObjectOutputStream(client.getOutputStream());
			MRMessage msg = (MRMessage) inStream.readObject();
			if (msg.getCommand() == Command.TASK) {
				System.out.println("Received a new task");
				executeTask((Task) msg.getPayload());
			}
			//inStream.close();
			//outStream.close();
			//client.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}
}
