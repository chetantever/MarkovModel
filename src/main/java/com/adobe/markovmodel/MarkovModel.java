package com.adobe.markovmodel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.adobe.dbconnector.MongoDBConnector;

public class MarkovModel extends Configured implements Tool {

	private static final String OUTPUT_PATH1 = "/Markov/processing/intermediate_output1";
	private static final String OUTPUT_PATH2 = "/Markov/processing/intermediate_output2";
	private static final String FILE_NAME = "/part-r-00000";
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MarkovModel(), args);
		System.exit(res);
	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		
		//To calculate numerator
		Job job1 = new Job(getConf(), "Markov Transition Probability Model");
		job1.setJarByClass(MarkovModel.class);
				
		job1.setMapperClass(MarkovModelMapper.class);
		job1.setReducerClass(MarkovModelReducer.class);
								
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH1));
		
		job1.waitForCompletion(true);

		//To calculate denominator
		Job job2 = new Job(getConf(), "Markov Transition Probability Model");
		job2.setJarByClass(MarkovModel.class);
						
		job2.setMapperClass(TransitionMapper.class);
		job2.setReducerClass(MarkovModelReducer.class);
								
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH1));
		FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH2));
		
		job2.waitForCompletion(true); 
		
		//To calculate transistion probability
		Job job3 = new Job(getConf(), "Markov Transition Probability Model");
		Path cachePath =  new Path(OUTPUT_PATH2+FILE_NAME);
		DistributedCache.addCacheFile(cachePath.toUri(), job3.getConfiguration());
				
		job3.setJarByClass(MarkovModel.class);
		job3.setJarByClass(MongoDBConnector.class);
		
		job3.setMapperClass(ProbabilityMapper.class);
		job3.setNumReduceTasks(0);
								
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(DoubleWritable.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job3, new Path(OUTPUT_PATH1));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		
		boolean success = job3.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
}
