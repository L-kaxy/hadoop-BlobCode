public class WordCount {
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		}

	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

		}

	}

	public static int run(String[] args) throws Exception {

		// get configuration
		Configuration configuration = new Configuration();

		// create job
		Job job = Job.getInstance(configuration, this.getClass().getSimpleName());

		// run jar
		job.setJarClass(this.getClass());

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