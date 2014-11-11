package ds.dfs.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class DFSFile implements Serializable{
	private static final long serialVersionUID = 1L;
	public String fileName;
	public int numPartitions;
	public HashMap<String, ArrayList<String>> partitionLoc;
	
	public DFSFile(String f, int n, HashMap<String, ArrayList<String>> host) {
		fileName = f;
		numPartitions = n;
		partitionLoc = host;
	}
}
