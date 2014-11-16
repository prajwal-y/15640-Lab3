package ds.mapreduce.commandline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import ds.dfs.dfsclient.DFSClient;
import ds.mapreduce.Common.ClusterStatus;
import ds.mapreduce.Common.Command;
import ds.mapreduce.Common.Constants;
import ds.mapreduce.Common.JobSubmission;
import ds.mapreduce.Common.MRMessage;

public class CommandLine {
	private static String jobTrackerHostName;
	private static String nameNodeHostName;
	
	private static void parseConfigFile() {
		File config = new File("mr.xml");
		String line;
		HashMap<String, String> configValues = new HashMap<String, String>();
		try {
			BufferedReader fileReader = new BufferedReader(new FileReader(
					config));
			while ((line = fileReader.readLine()) != null) {
				String[] kv = line.split("=");
				String key = kv[0];
				String value = kv[1];
				configValues.put(key, value);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		jobTrackerHostName = configValues.get("JOBTRACKERHOST");
		nameNodeHostName = configValues.get("NAMENODEHOST");
	}
	
	private static void printUsage(){
		System.out.println("This is the command line program of MapReduce");
		System.out.println("This system can be used to submit jobs and get status of jobs and cancel(?) jobs");
		System.out.println("Usage:");
		System.out.println("cl.jar [OPTIONS] [FILES]");
		System.out.println("No options: Prints this message");
		System.out.println("-status Print the status of currently running job on the map reduce framework");
		System.out.println("-submit <input location(afs/dfs)> <output location on dfs> <map/reduce jar implemented by user>: Submit a job to the cluster");
		System.exit(0);
	}
	
	public static void main(String[] args){
		parseConfigFile();
		if(args.length == 0)
			printUsage();
		if(args[0].equals("-status")){
			if(args.length != 1)
				printUsage();
			Socket jSocket;
			try {
				jSocket = new Socket(jobTrackerHostName, Constants.JOBTRACKER_PORT);
				MRMessage msg = new MRMessage(Command.LISTJOBS, null);
				ObjectOutputStream out = new ObjectOutputStream(jSocket.getOutputStream());
				out.writeObject(msg);
				out.flush();
				ObjectInputStream in = new ObjectInputStream(jSocket.getInputStream());
				MRMessage result = (MRMessage) in.readObject();
				ClusterStatus status = (ClusterStatus) result.getPayload();
				HashMap<String, ArrayList<Integer>> jobData = status.getClusterStatus();
				System.out.println("Jobs currently running : " + jobData.keySet().size());
				for (String job : jobData.keySet()){
					ArrayList<Integer> taskCount = jobData.get(job);
					System.out.println("Job ID: " + job);
					System.out.println("\tTotal number of map tasks : " + taskCount.get(0));
					System.out.println("\tTotal number of reduce tasks : " + taskCount.get(1));
					System.out.println("\tMap tasks currently running : " + taskCount.get(2));
					System.out.println("\tReduce tasks currently running : " + taskCount.get(3));
					System.out.println("\tPending map tasks : "  + (taskCount.get(0) - taskCount.get(2)));
					System.out.println("\tPending reduce tasks : " + (taskCount.get(1) - taskCount.get(3)));
					System.out.println();
				}
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		if(args[0].equals("-submit")){
			if(args.length != 4)
				printUsage();
			String inputPath = args[1];
			String outputPath = args[2];
			String jarPath = args[3];
			DFSClient client = new DFSClient(nameNodeHostName);
			try {
				client.copyToDFS(inputPath, "DFS://", true);
				client.copyToDFS(jarPath, "DFS://", false);
			} catch (ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {				
				String[] pathSplit = inputPath.split("/");
				String dfsInputPath = "DFS://" + pathSplit[pathSplit.length - 1];
				pathSplit = jarPath.split("/");
				String dfsJarPath = "DFS://" + pathSplit[pathSplit.length - 1];
				JobSubmission submission = new JobSubmission(dfsInputPath, outputPath, dfsJarPath);
				
				Socket jSocket = new Socket(jobTrackerHostName, Constants.JOBTRACKER_PORT);
				MRMessage msg = new MRMessage(Command.SUBMIT, submission);
				ObjectOutputStream out = new ObjectOutputStream(jSocket.getOutputStream());
				out.writeObject(msg);
				out.flush();
				ObjectInputStream in = new ObjectInputStream(jSocket.getInputStream());
				in.readObject();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
