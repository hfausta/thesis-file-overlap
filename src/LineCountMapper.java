import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class LineCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String fileName = fileSplit.getPath().getName().trim();
		context.write(new Text(fileName), new Text("one"));
	}

}
