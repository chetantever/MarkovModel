package com.adobe.markovmodel;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MarkovModelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static String ROW_DELIM = ",";
	private static int SKIP_HEADER = 1;
	private IntWritable mapValue  = new IntWritable(1);
	
	private String[] items;
	Text mapKey = new Text();
		
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		items = value.toString().split(ROW_DELIM);
		if (items.length >= (SKIP_HEADER + 2)) {
        	for (int i = SKIP_HEADER + 1; i < items.length; ++i) {
        		mapKey.set(items[i-1]+ROW_DELIM+ items[i]);
        		context.write(mapKey, mapValue);
        	}
    	}
		
	}
}
