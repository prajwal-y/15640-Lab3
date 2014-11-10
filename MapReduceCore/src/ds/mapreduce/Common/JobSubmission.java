package ds.mapreduce.Common;

import java.io.Serializable;

public class JobSubmission implements Serializable {
	private static final long serialVersionUID = 1L;
	private String inputPath;
	private String outputPath;
	private String jarPath;

	public JobSubmission(String iPath, String oPath, String jPath) {
		inputPath = iPath;
		outputPath = oPath;
		jarPath = jPath;
	}

	public String getInputPath() {
		return inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public String getJarPath() {
		return jarPath;
	}

}