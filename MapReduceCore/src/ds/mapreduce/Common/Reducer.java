package ds.mapreduce.Common;

import java.util.ArrayList;

public class Reducer extends Thread {
	private ReduceRecordReader reader;
	private ReduceOutputCollector collector;
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
	
	public Reducer(ReduceRecordReader r, ReduceOutputCollector c){
		reader = r;
		collector = c;
	}

	public void run() {
		String nextRecord;
		
		String currentKey = null;
		ArrayList<String> values = new ArrayList<String>();
		try {
			while ((nextRecord = reader.nextRecord()) != null) {
				String key = nextRecord.split(" ")[0];
				String value = nextRecord.split(" ")[1];
				System.out.println(key + " " + value);
				if (currentKey == null || !key.equals(currentKey)) {
					if(currentKey != null)
						reduce(currentKey, values, collector);
					currentKey = key;
					values.clear();
					values.add(value);
				} else {
					values.add(value);
				}
			}
			if(currentKey != null)
				reduce(currentKey, values, collector);
			collector.flush();
		} finally {
			cleanup();
		}
	}
}
