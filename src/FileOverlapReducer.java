import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FileOverlapReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	public void reduce (IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for(Text v : values) {
			context.write(key, v);
		}
	}
}
