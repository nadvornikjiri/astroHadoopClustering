package astro.hadoop.cluster;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Placeholder for reducer class. At the moment more like an IdentityReducer,
 * can be used to aggregate information about clusters - these are already
 * produced by the mapper.
 * 
 * @author caucau
 */
public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(key, value);
		}
	}
}
