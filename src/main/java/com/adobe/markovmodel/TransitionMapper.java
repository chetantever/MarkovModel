package com.adobe.markovmodel;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransitionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static String ROW_DELIM = "\t";
	private static String KEYDELIM = ",";
	private String[] items;
	private String[] keys;
	
	Text mapKey = new Text();
	IntWritable mapValue = new IntWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {
			items = value.toString().split(ROW_DELIM);
			keys = items[0].toString().split(KEYDELIM);
			mapKey.set(keys[0]);
			mapValue.set(Integer.parseInt(items[1]));
			context.write(mapKey, mapValue);
		} catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
