package ds.mapreduce.Common;


public class Mapper extends Thread {
	MapRecordReader reader;
	MapOutputCollector collector;
	
	public void map(String record, MapOutputCollector collector) {
		String key = null;
		String value = null;
		collector.writeToBuffers(key, value);
	}
	
	public Mapper(MapRecordReader r, MapOutputCollector c){
		reader = r;
		collector = c;
	}

	public void cleanup() {
		// Nothing here
	}

	public void run() {
		String nextRecord;
		try {
			while ((nextRecord = reader.nextRecord()) != null) {
				map(nextRecord, collector);
			}
			collector.flush();
		} finally {
			cleanup();
		}
	}
}
