import java.io.IOException;
import org.json.JSONObject;
import java.lang.String;
import java.io.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RedditAverage extends Configured implements Tool {
	
	public static class RedditAverageMapper 
	extends Mapper<LongWritable,Text,Text,LongPairWritable>{
		private final static LongPairWritable longpair = new LongPairWritable();
		private Text word= new Text();	

			public void map	(LongWritable key,Text value,Context context)
				throws IOException, InterruptedException
			{
			JSONObject record= new JSONObject(value.toString());
			String subreddit_key=(String) record.get("subreddit");
			int score_key=(int) record.get("score");
			word.set(subreddit_key);
			longpair.set(1,score_key);
			context.write(word,longpair);
		      }
	}

	public static class RedditAverageReducer
	extends Reducer<Text,LongPairWritable,Text,DoubleWritable>{
		private DoubleWritable result= new DoubleWritable();
		
			public void reduce(Text key,Iterable<LongPairWritable>values,Context context)
				throws IOException, InterruptedException

				{
					double comment_count=0;
					double total_score=0;
					for(LongPairWritable val:values)
						{
							comment_count +=val.get_0();
							total_score +=val.get_1();						
						}
					double average=total_score/comment_count;
					result.set(average);
					context.write(key,result);				
				}	
	}

	public static class RedditAverageCombiner
	extends Reducer<Text,LongPairWritable,Text,LongPairWritable>{
		 private LongPairWritable result= new LongPairWritable();
		
			public void reduce(Text key,Iterable<LongPairWritable>values,Context context)
				throws IOException, InterruptedException

				{
					int comment_count=0;
					int total_score=0;
					for(LongPairWritable val:values)
						{
							comment_count +=val.get_0();
							total_score +=val.get_1();						
						}
					result.set(comment_count,total_score);
					
					context.write(key,result);				
				}	
	}
	







public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Reddit");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(RedditAverageMapper.class);
		job.setCombinerClass(RedditAverageCombiner.class);
		job.setReducerClass(RedditAverageReducer.class);


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

	
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}


