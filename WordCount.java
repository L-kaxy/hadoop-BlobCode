package com.l_kaxy.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job1;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class WordCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text mapOutputkey = new Text();
		private final static IntWritable mapOutputValue = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();

			// split
			StringTokenizer stringTokenizer = new StringTokenizer(lineValue);

			// iterator
			while (stringTokenizer.hasMoreTokens()) {
				// word
				String wordValue = stringTokenizer.nextToken();
				// set key
				mapOutputkey.set(wordValue);
				// output
				context.write(mapOutputkey, mapOutputValue);
			}
		}

	}

	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable mapOutputValue = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			// iterator
			for (IntWritable value : values) {
				sum += value.get();
			}

			// set value
			mapOutputValue.set(sum);
			
			// output
			context.write(key, mapOutputValue);
		}

	}

	public int run(String[] args) throws Exception {

		// get configuration
		Configuration configuration = new Configuration();

		// create job
		Job job = Job.getInstance(configuration, this.getClass()
				.getSimpleName());

		// run jar
		job.setJarByClass(this.getClass());

		// input
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);

		// map
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reduce
		job.setReducerClass(WordCountReducer.class);
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
		int status = new WordCount().run(args);

		System.exit(status);
	}

}
