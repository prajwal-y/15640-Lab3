package ds.mapreduce.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import ds.mapreduce.JobTracker.JobTracker;
import ds.mapreduce.TaskTracker.TaskTracker;

public class Initialize {
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

	public static void main(String[] args) {
		parseConfigFile();
		
		if (args[0].equals("-jobtracker")) {
			System.out.println("This machine is the JobTracker");
			System.out.println("Starting JobTracker...");
			System.out.println("JobTracker started");
			JobTracker jTracker = new JobTracker(nameNodeHostName);
			jTracker.startEventLoop();
		} else {
			System.out.println("This machine is a TaskTracker");
			System.out.println("Starting TaskTracker...");
			System.out.println("TaskTracker started");
			TaskTracker tTracker = new TaskTracker(jobTrackerHostName,
					nameNodeHostName);
			tTracker.startEventLoop();
		}
	}
}
