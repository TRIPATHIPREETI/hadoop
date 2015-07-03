# hadoop
haddop programs
------------------------------------------------------------------------------------
package com.hadoop.com.wordcount;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class stopwords {

	static Set <String> Stopwords = new HashSet<String>();


	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		// private final static IntWritable one = new IntWritable(1);
		//private Text word = new Text();
		IntWritable inW=new IntWritable(1);
		Text tr=new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException 
		{
			FileReader reader2 = new FileReader(
					"/home/impadmin/Desktop/stopwordslist.txt");
			BufferedReader Reader = new BufferedReader(reader2);
			
			String line;
			try {
				while ((line = Reader.readLine()) != null) {
					Stopwords.add(line);
					//System.out.println(line);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) 
			{
				String temp=itr.nextToken();
				System.out.println(temp);
				if(Stopwords.contains(temp))
				{	
					tr.set(temp);
					context.write(tr, inW);
				}
			}
		}
	}
	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		

		Configuration conf = new Configuration();
		Job job = new Job(conf, "word count");
		job.setJarByClass(stopwords.class);
		job.setMapperClass(TokenizerMapper.class);
	//	job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(
				"/user/impadmin/inputfile.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/user/impadmin/outputfile.txt"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
