import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FileOverlap {

	public static boolean LineCountJob(String[] args, ArrayList<Long> lineCount) throws IOException, InterruptedException, ClassNotFoundException {
		JobConf conf = new JobConf();
		
		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(1);
		conf.setInt("overlap", Integer.parseInt(args[1]));
		
		Job job = new Job(conf);
		job.setJarByClass(FileOverlap.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    
	    job.setMapperClass(LineCountMapper.class);
	    job.setReducerClass(LineCountReducer.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[0] + "-LineCount"));
	    
	    boolean success = job.waitForCompletion(true);
	    
	    CounterGroup result = job.getCounters().getGroup("LineCount");
		for(int i=0;i<result.size();i++) {
			lineCount.add(job.getCounters().findCounter("LineCount", String.valueOf(i)).getValue());
		}
		
	    return success;
	}
	
	public static boolean FileOverlapJob(String[] args, ArrayList<Long> lineCount) throws IOException, InterruptedException, ClassNotFoundException {
		JobConf conf = new JobConf();
		
		FileSystem fs = FileSystem.get(conf);
	    Path dir = new Path(args[0]);
	    FileStatus[] stats = fs.listStatus(dir);
        conf.setInt("numFiles", stats.length);
        conf.setInt("overlap", Integer.parseInt(args[1]));
		conf.setNumMapTasks(5);
		conf.setNumReduceTasks(5);
		
		for(int i=0;i<lineCount.size();i++) {
			conf.set(String.valueOf(i), String.valueOf(lineCount.get(i)));
		}
		
		Job job = new Job(conf);
		job.setJarByClass(FileOverlap.class);
	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);

	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    
	    job.setMapperClass(FileOverlapMapper.class);
	    job.setReducerClass(FileOverlapReducer.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[0] + "-Output"));
	    
	    boolean success = job.waitForCompletion(true);
	    return success;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		ArrayList<Long> lineCount = new ArrayList<Long>();
		if(LineCountJob(args, lineCount)) {
			if(FileOverlapJob(args, lineCount)) {
				System.exit(0);
			}
		}
	}

}
