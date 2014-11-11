package ds.dfs.util;

import java.io.Serializable;

public class FileObject implements Serializable {
	
	private static final long serialVersionUID = 1L;
	String file;
	String folderPathInDFS;
	boolean isSplittable;
	
	public FileObject(String f, String folder, boolean b) {
		file = f;
		folderPathInDFS = folder;
		isSplittable = b;
	}

}
