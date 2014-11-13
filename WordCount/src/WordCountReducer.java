import java.util.ArrayList;

import ds.mapreduce.Common.ReduceOutputCollector;
import ds.mapreduce.Common.ReduceRecordReader;
import ds.mapreduce.Common.Reducer;


public class WordCountReducer extends Reducer {

	public WordCountReducer(ReduceRecordReader r, ReduceOutputCollector c) {
		super(r, c);
	}

	@Override
	public void reduce(String key, ArrayList<String> values,
			ReduceOutputCollector collector) {
		String outputKey = key;
		String outputValue = ((Integer) values.size()).toString();
		collector.writeToBuffers(outputKey, outputValue);
	}
}
