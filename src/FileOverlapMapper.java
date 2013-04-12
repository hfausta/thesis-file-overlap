import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class FileOverlapMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String fileName = fileSplit.getPath().getName().trim();
		String[] fileNameSplit = fileName.split("part-r-");
		int outKey = Integer.parseInt(fileNameSplit[1]);
		int numFiles = context.getConfiguration().getInt("numFiles", 0);
		int nextKey = outKey + 1;
		
		if(nextKey == numFiles) {
			nextKey = 0;
		}
		
		int overlap = context.getConfiguration().getInt("overlap", 0);
		int numLines = Integer.parseInt(context.getConfiguration().get(String.valueOf(nextKey), "0"));
		
		if((overlap > 0) && (numLines > 0)) {
			//System.out.println(nextKey + "     " + value);
	    	context.write(new IntWritable(nextKey), value);
	    	numLines--;
	    	context.getConfiguration().set(String.valueOf(nextKey), String.valueOf(numLines));
		}
		context.write(new IntWritable(outKey), value);
	}
}
