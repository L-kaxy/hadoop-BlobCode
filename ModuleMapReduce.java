package com.l_kaxy.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ModuleMapReduce extends Configured implements Tool {

	// TODO:
	public static class ModuleMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			// Nothing
		}
  
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO:
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			// Nothing
		}

	}

	// TODO:
	public static class ModuleReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			// Nothing
		}
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// TODO:
		}
		
		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			// Nothing
		}

	}

	public int run(String[] args) throws Exception {

		// get configuration
		Configuration configuration = getConf();

		// create job
		Job job = Job.getInstance(configuration, this.getClass()
				.getSimpleName());

		// run jar
		job.setJarByClass(this.getClass());

		// input
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);

		// map
		job.setMapperClass(ModuleMapper.class);
		// TODO:
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reduce
		job.setReducerClass(ModuleReducer.class);
		// TODO:
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// output
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);

		// submit
		Boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		// get configuration
		Configuration configuration = new Configuration();

		// int status = new WordCountMapReduce().run(args);
		int status = ToolRunner.run(configuration, new ModuleMapReduce(), args);

		System.exit(status);
	}

}
