package ds.mapreduce.Common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class MapRecordReader {
	String path;
	long currentRecord;
	BufferedReader fileReader;
	
	
	private void openFile(){
		//Use DFS to open file
		File file = new File(path);
		try {
			BufferedReader fileReader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public MapRecordReader(String p){
		path = p;
		openFile();
		currentRecord = 0;
	}
	
	public String nextRecord(){
		String record = null;
		try {
			record = fileReader.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		currentRecord++;
		return record;		
	}
}
