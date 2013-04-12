import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class LineCountReducer extends Reducer<Text, Text, IntWritable, LongWritable> {
	
	public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long lineCount = 0;
		
		for(Text v : values) {
			lineCount++;
		}
		
		int overlap = context.getConfiguration().getInt("overlap", 0);
		lineCount = overlap*lineCount/100;
		
		String[] keySplit = key.toString().split("part-r-");
		int outKey = Integer.parseInt(keySplit[1]);
		
		context.getCounter("LineCount", String.valueOf(outKey)).increment(lineCount);
		context.write(new IntWritable(outKey), new LongWritable(lineCount));
	}

}
