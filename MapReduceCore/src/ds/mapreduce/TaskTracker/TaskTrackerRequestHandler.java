package ds.mapreduce.TaskTracker;

import java.io.Console;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import ds.mapreduce.Common.Command;
import ds.mapreduce.Common.MRMessage;
import ds.mapreduce.Common.MapOutputCollector;
import ds.mapreduce.Common.MapRecordReader;
import ds.mapreduce.Common.Mapper;
import ds.mapreduce.Common.ReduceOutputCollector;
import ds.mapreduce.Common.ReduceRecordReader;
import ds.mapreduce.Common.Reducer;
import ds.mapreduce.Common.TaskType;
import ds.mapreduce.JobTracker.ReduceTask;
import ds.mapreduce.JobTracker.MapTask;
import ds.mapreduce.JobTracker.Task;

public class TaskTrackerRequestHandler extends Thread{
	private Socket client;
	private ObjectInputStream inStream = null;
	private ObjectOutputStream outStream = null;
	private TaskTracker tracker;
	
	public TaskTrackerRequestHandler(Socket c, TaskTracker t){
		client = c;
		tracker = t;
	}
	
	private void executeTask(Task t){
		tracker.setIdle(false);
		String path = null;
		//DFS copy program jar to local /tmp/somewhere
		//DFS copy input file to local /tmp/somewhere
		JarFile jar;
		try {
			jar = new JarFile(path);
			Enumeration e = jar.entries();

			URL[] urls = { new URL("jar:file:" + path+"!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

			    while (e.hasMoreElements()) {
			        JarEntry je = (JarEntry) e.nextElement();
			        if(je.isDirectory() || !je.getName().endsWith(".class")){
			            continue;
			        }
			    // -6 because of .class
			    String className = je.getName().substring(0,je.getName().length()-6);
			    className = className.replace('/', '.');
			    Class c = cl.loadClass(className);
			    if(c.isAssignableFrom(Class.forName("Mapper")) || c.isAssignableFrom(Class.forName("Reducer"))){
			    	if(t.getTaskType() == TaskType.MAP){
			    		MapRecordReader mReader = new MapRecordReader(((MapTask)t).getInputPath());
			    		MapOutputCollector mCollector = new MapOutputCollector(t.getOutputPath(), 10, t.getJobId(),
			    				Integer.parseInt(t.getTaskId()));
			    		Mapper mapper = (Mapper) c.getConstructor(MapRecordReader.class, ReduceOutputCollector.class)
			    				.newInstance(mReader, mCollector);
			    		new Thread(mapper).start();
			    	}
			    	else if(t.getTaskType() == TaskType.REDUCE){
			    		ReduceRecordReader rReader = new ReduceRecordReader(((ReduceTask)t).getPartitionId(), t.getJobId());
			    		ReduceOutputCollector rCollector = new ReduceOutputCollector(t.getOutputPath());
			    		Reducer reducer = (Reducer) c.getConstructor(MapRecordReader.class, ReduceOutputCollector.class)
			    				.newInstance(rReader, rCollector);
			    		new Thread(reducer).start();
			    	}
			    }
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InstantiationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NoSuchMethodException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SecurityException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalArgumentException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvocationTargetException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	public void run() {
		try {
			inStream = new ObjectInputStream(client.getInputStream());
			outStream = new ObjectOutputStream(client.getOutputStream());
			MRMessage msg = (MRMessage) inStream.readObject();
			if(msg.getCommand() == Command.TASK){
				System.out.println("Received a new task");
				executeTask((Task)msg.getPayload());				
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
