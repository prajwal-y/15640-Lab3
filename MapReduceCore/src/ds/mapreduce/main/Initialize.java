package ds.mapreduce.main;
import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import ds.mapreduce.JobTracker.JobTracker;
import ds.mapreduce.TaskTracker.TaskTracker;

public class Initialize {
	private static String jobTrackerHostName;
	private static String currentHostName;

	private static void parseConfigFile() {
		Document doc = null;
		try {
			File config = new File("/usr/local/mr.xml");
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			doc = db.parse(config);
			doc.getDocumentElement().normalize();
		} catch (ParserConfigurationException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (SAXException e) {
			System.out.println(e.getMessage());
		}
		NodeList nodeLst = doc.getElementsByTagName("jobtracker");
		jobTrackerHostName = nodeLst.item(0).getNodeValue();
		
	}

	public static void main(String[] args) {
		//parseConfigFile();
		try {
			currentHostName = Runtime.getRuntime().exec("hostname").getOutputStream().toString();
			System.out.println("Current host is: " + currentHostName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//if(currentHostName.equals(jobTrackerHostName)){
		if(args[0].equals("jobtracker")){
			System.out.println("This machine is the JobTracker");
			System.out.println("Starting JobTracker...");
			System.out.println("JobTracker started");
			JobTracker jTracker = new JobTracker();
			jTracker.startEventLoop();
		}
		else{
			System.out.println("This machine is a TaskTracker");
			System.out.println("Starting TaskTracker...");
			System.out.println("TaskTracker started");
			TaskTracker tTracker = new TaskTracker(jobTrackerHostName, "127.0.0.1");
			tTracker.startEventLoop();
		}
	}
}
