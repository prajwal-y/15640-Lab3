package ds.mapreduce.Common;

import java.util.ArrayList;

public class Reducer extends Thread {
	public void reduce(String key, ArrayList<String> values,
			ReduceOutputCollector collector) {
		String outputKey = key;
		for (String value : values){
			collector.writeToBuffers(outputKey, value);
		}
		
	}

	public void cleanup() {
		// Nothing here
	}

	public void run(ReduceRecordReader reader, ReduceOutputCollector collector) {
		String nextRecord;
		
		String currentKey = null;
		ArrayList<String> values = new ArrayList<String>();
		try {
			while ((nextRecord = reader.nextRecord()) != null) {
				String key = nextRecord.split(" ")[0];
				String value = nextRecord.split(" ")[1];
				if (currentKey.isEmpty() || !key.equals(currentKey)) {
					reduce(currentKey, values, collector);
					currentKey = key;
					values.clear();
					values.add(value);
				} else {
					values.add(value);
				}
			}
		} finally {
			cleanup();
		}
	}
}
