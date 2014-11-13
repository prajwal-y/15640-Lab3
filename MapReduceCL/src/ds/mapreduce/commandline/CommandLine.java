package ds.mapreduce.commandline;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import ds.dfs.dfsclient.DFSClient;
import ds.mapreduce.Common.Command;
import ds.mapreduce.Common.Constants;
import ds.mapreduce.Common.JobSubmission;
import ds.mapreduce.Common.MRMessage;

public class CommandLine {
	private static void printUsage(){
		System.out.println("This is the command line program of MapReduce");
		System.out.println("This system can be used to submit jobs and get status of jobs and cancel(?) jobs");
		System.out.println("Usage:");
		System.out.println("cl.jar [OPTIONS] [FILES]");
		System.out.println("No options: Prints this message");
		System.out.println("-status Print the status of currently running job on the map reduce framework");
		System.out.println("-submit <input location(afs/dfs)> <output location on dfs> <map/reduce jar implemented by user>: Submit a job to the cluster");
		System.out.println("-copyFromLocal <local location(afs)> <location on dfs>: Copy a file into the dfs");
		System.out.println("-copyToLocal <location on dfs> <local location(afs)>: Copy a file from the dfs to the local filesystem");
		System.out.println("-copy <input location on dfs> <output location on dfs>: Copy files within the dfs");
		System.out.println("-fs Prints current dfs folder hierarchy");
		System.exit(0);
	}
	
	public static void main(String[] args){
		if(args.length == 0)
			printUsage();
		if(args[0].equals("-status")){
			if(args.length != 1)
				printUsage();
			//Get status from JobTracker
			//Parse and print JobTracker data
		}
		if(args[0].equals("-submit")){
			if(args.length != 4)
				printUsage();
			String inputPath = args[1];
			String outputPath = args[2];
			String jarPath = args[3];
			DFSClient client = new DFSClient("127.0.0.1");
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
				
				Socket jSocket = new Socket("127.0.0.1", Constants.JOBTRACKER_PORT);
				MRMessage msg = new MRMessage(Command.SUBMIT, submission);
				ObjectOutputStream out = new ObjectOutputStream(jSocket.getOutputStream());
				out.writeObject(msg);
				
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(args[0].equals("-copyFromLocal")){
			if(args.length != 3)
				printUsage();
			//Use DFS library to copy data into DFS
			//Print exceptions or success as required
		}
		if(args[0].equals("-copyToLocal")){
			if(args.length != 3)
				printUsage();
			//Use DFS library to copy data into local system
			//Print exceptions or success as required
		}
		if(args[0].equals("-copy")){
			if(args.length != 3)
				printUsage();
			//Use DFS library to copy data into DFS
			//Print exceptions or success as required
		}
		if(args[0].equals("-fs")){
			if(args.length != 1)
				printUsage();
			//Use DFS library to get DFS state
			//Print state
		}
	}
}
