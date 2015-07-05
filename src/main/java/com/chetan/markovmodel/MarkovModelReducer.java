package com.chetan.markovmodel;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MarkovModelReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private int count;
	private IntWritable outVal = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		outVal.set(count);
		context.write(key, outVal);
	}
}
