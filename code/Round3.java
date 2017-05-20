package ecp.bigdata.Tutorial1;
//DO SET of doc1, set of doc2, check if word in intersection if true 2, else 1
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Round3 extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new Round3(),
				args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "Round3");

		job.setJarByClass(Round1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
		job.setCombinerClass(Combine.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", "#");
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text outputval = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			for (String token : value.toString().split("\\r?\\n")) {
				String[] parts = token.split("#");

				word.set(parts[1]);
				outputval.set(parts[0]+"#"+parts[2]+"#"+parts[3]);
				context.write(word, outputval);
			}
		}
	}
	public static HashMap<String,Double> Docs_per_Word = new HashMap<String, Double>();
	public static class Combine extends Reducer<Text, Text, Text, Text> {
		
		@Override
	    public void reduce(Text key, Iterable<Text> value, Context context)
	            throws IOException, InterruptedException {
	    	
			double sum = 0;
			for (Text val : value){
				sum += 1;
				context.write(key,val);
			}
			Docs_per_Word.put(key.toString(),sum);
		}
	}
	
	public static class Reduce extends
			Reducer<Text, Text, Text, Text> {
		private Text outputval = new Text();
		private Text newkey = new Text();
		String parts[];
		@Override
		public void reduce(Text key,  Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			HashMap<String, String> hmap = new HashMap<String, String>();
			double wordcount;
			double wordsperdoc;
			double docsperword;
			double totaldocs=2;
			for (Text value : values) {
					parts = value.toString().split("#");
					
					wordcount = Double.valueOf(parts[1]);
					wordsperdoc = Double.valueOf(parts[2]);
					docsperword = Docs_per_Word.get(key.toString());
					double TFIDF;
					TFIDF  = (wordcount/wordsperdoc)*Math.log(totaldocs/docsperword);
					outputval.set(Double.toString(TFIDF));
					newkey.set(key.toString()+"|"+parts[0]);
					context.write(newkey, outputval);
			}

		}
	}
}
