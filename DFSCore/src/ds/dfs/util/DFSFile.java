package ds.dfs.util;

import java.io.Serializable;

public class DFSFile implements Serializable{
	private static final long serialVersionUID = 1L;
	public String fileName;
	public String partitionId;
	public String dfsFilePath;
	public String dataNodeHost;
	
	public DFSFile(String f, String path, String host) {
		fileName = f;
		dfsFilePath = path;
		dataNodeHost = host;
	}
}
