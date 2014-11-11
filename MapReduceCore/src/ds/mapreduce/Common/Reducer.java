package ds.mapreduce.Common;

import java.util.ArrayList;

public class Reducer extends Thread {
	public void reduce(String key, ArrayList<String> values,
			OutputCollector collector) {
		String outputKey = key;
		for (String value : values){
			collector.write(outputKey, value);
		}
		
	}

	public void cleanup() {
		// Nothing here
	}

	public void run(RecordReader reader, String path) {
		String nextRecord;
		OutputCollector collector = new OutputCollector(path); // TODO get
																// location to
																// write to
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
