package ds.dfs.util;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class DataNodeMetadata implements Serializable{
	private static final long serialVersionUID = 1L;
	public String host;
	public String dataNodeId;
	public Map<String, List<Integer>> fileMap;
	
	public DataNodeMetadata(String h, String id, Map<String, List<Integer>> map) {
		host = h;
		dataNodeId = id;
		fileMap = map;
	}
}
