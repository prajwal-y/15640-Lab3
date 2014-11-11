package ds.mapreduce.Common;


public class Mapper extends Thread {
	public void map(String record, OutputCollector collector) {
		String key = null;
		String value = null;
		collector.write(key, value);
	}

	public void cleanup() {
		// Nothing here
	}

	public void run(RecordReader reader, String path) {
		String nextRecord;
		OutputCollector collector = new OutputCollector(path); // TODO get location
															// to write to
		try {
			while ((nextRecord = reader.nextRecord()) != null) {
				map(nextRecord, collector);
			}
		} finally {
			cleanup();
		}
	}
}
