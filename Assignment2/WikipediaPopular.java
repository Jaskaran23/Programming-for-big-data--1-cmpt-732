import java.io.IOException;

import java.lang.String;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {
	
	public static class WikipediaPopularMapper 
	extends Mapper<LongWritable,Text,Text,LongWritable>{
		
		private Text word= new Text();
			

			public void map	(LongWritable key,Text value,Context context)
				throws IOException, InterruptedException
			{
			String wikipedia_data=value.toString();
			String[] data_value=wikipedia_data.split("\\s+");
			String lang ="en";
			String m="Main_Page";

			
			if(data_value[1].equals(lang))
			{
				if(!(data_value[2].equals(m)) && !(data_value[2].startsWith("Special:")))
				{
				word.set(data_value[0]);
				context.write(word,new LongWritable(Long.parseLong(data_value[3])));
				}
				
			}
			

		      }
	}



	public static class WikipediaPopularReducer
	extends Reducer<Text,LongWritable,Text,LongWritable>{
		private LongWritable result= new LongWritable();
		
			public void reduce(Text key,Iterable<LongWritable>values,Context context)
				throws IOException, InterruptedException

				{
					long count1=0;
					long count2=0;
					for(LongWritable val:values)
						{
							count1=val.get();
							if(count1>count2)
								{
								count2=count1;
								}
							else
								{
								continue;
								}
														
						}
					
					result.set(count2);
					context.write(key,result);				
				}	
	}




public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Wikipedia");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(WikipediaPopularMapper.class);
		job.setCombinerClass(WikipediaPopularReducer.class);
		job.setReducerClass(WikipediaPopularReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
	

	
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}



