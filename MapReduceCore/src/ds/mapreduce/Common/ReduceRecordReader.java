package ds.mapreduce.Common;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;

import ds.dfs.dfsclient.DFSClient;

public class ReduceRecordReader {
	int partitionId;
	String jobId;
	int currentRecord;
	ArrayList<String> sortedBuffer;
	int length;
	String nameNodeHost;

	private void openAndSort() {
		// Use DFS to open jId/partitionId folder
		// Open each file in above folder and sort and store in buffer
		DFSClient dfsClient = new DFSClient(nameNodeHost);
		String partOutputPath = "DFS://" + jobId + "/" + partitionId;
		ArrayList<String> mapOutputs = null;
		try {
			mapOutputs = dfsClient
					.listFilesinDirectory(partOutputPath);
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		File file = null;
		FileInputStream fstream;
		DataInputStream in;
		BufferedReader br;
		String strLine;
		for (String output : mapOutputs) {
			try {
				file = dfsClient.openFile("DFS://" + output, false);
			} catch (ClassNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			try {
				fstream = new FileInputStream(file);
				in = new DataInputStream(fstream);
				br = new BufferedReader(new InputStreamReader(in));
				while ((strLine = br.readLine()) != null) {
					sortedBuffer.add(strLine);
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		length = sortedBuffer.size(); 
		Collections.sort(sortedBuffer);
	}

	public ReduceRecordReader(int pId, String jId, String nNodeHost) {
		partitionId = pId;
		jobId = jId;
		sortedBuffer = new ArrayList<String>();
		openAndSort();
		currentRecord = 0;
		nameNodeHost = nNodeHost;
	}

	public String nextRecord() {
		String record = null;
		if (currentRecord < length) {
			record = sortedBuffer.get((int) currentRecord);
			currentRecord++;
		}
		return record;
	}
}
