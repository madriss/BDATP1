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

public class Round2 extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new Round2(),
				args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "Round2");

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
				outputval.set(parts[0]+"#"+parts[2]);
				context.write(word, outputval);
			}
		}
	}
	public static HashMap<String,String> Words_per_Doc = new HashMap<String, String>();
	public static class Combine extends Reducer<Text, Text, Text, Text> {
		
		@Override
	    public void reduce(Text key, Iterable<Text> value, Context context)
	            throws IOException, InterruptedException {
	    	
			int sum = 0;
			for (Text val : value){
				String[] wordcount = val.toString().split("#");
				sum += Integer.valueOf(wordcount[1]);
				context.write(key,val);
			}
			String TextID = key.toString();
			Words_per_Doc.put(TextID,Integer.toString(sum));
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
			for (Text value : values) {
					parts = value.toString().split("#");
					if(hmap.get(key.toString()+parts[0]) == null) {hmap.put(key.toString()+parts[0], "");}
					hmap.put(key.toString()+parts[0], parts[1]);
					outputval.set(hmap.get(key.toString()+parts[0])+"#"+Words_per_Doc.get(key.toString()));
					newkey.set(key.toString()+"#"+parts[0]);
					context.write(newkey, outputval);
			}

		}
	}
}
