import ds.mapreduce.Common.MapOutputCollector;
import ds.mapreduce.Common.MapRecordReader;
import ds.mapreduce.Common.Mapper;


public class WordCountMapper extends Mapper {

	public WordCountMapper(MapRecordReader r, MapOutputCollector c) {
		super(r, c);
	}

	@Override
	public void map(String record, MapOutputCollector collector) {
		String[] words = record.split(" ");
		for (String word : words) {
			collector.writeToBuffers(word, "1");
		}
	}
}
