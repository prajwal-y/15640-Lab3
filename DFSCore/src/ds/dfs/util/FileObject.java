package ds.dfs.util;

import java.io.Serializable;

public class FileObject implements Serializable {
	
	private static final long serialVersionUID = 1L;
	String file;
	boolean isSplittable;
	
	public FileObject(String f, boolean b) {
		file = f;
		isSplittable = b;
	}

}
