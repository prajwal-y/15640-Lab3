package ds.mapreduce.Common;

import java.io.IOException;
import java.util.ArrayList;

public class ReduceRecordReader {
	int partitionId;
	String jobId;
	long currentRecord;
	ArrayList<String> sortedBuffer;
	int length;

	private void openAndSort() {
		// Use DFS to open jId/partitionId folder
		// Open each file in above folder and sort and store in buffer
	}

	public ReduceRecordReader(int pId, String jId) {
		partitionId = pId;
		jobId = jId;
		openAndSort();
		currentRecord = 0;
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
