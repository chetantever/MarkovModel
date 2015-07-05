package com.chetan.markovmodel;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProbabilityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private static String ROW_DELIM = "\t";
	private static String KEY_DELIM = ",";
	
	KeyLookup keyLookup = new KeyLookup();
	
	Text mapKey = new Text();
	DoubleWritable mapValue = new DoubleWritable();
	
	
	public void setup(Context context) {
		try {
		 Path[] uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		 String filePath = uris[0].toString();
		 keyLookup.setLookupFilePath(filePath);
		 }
		catch(Exception e) {
			System.err.println("Could not get cache file");
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(ROW_DELIM);
		String[] lookup = line[0].split(KEY_DELIM);
		String lookupKey = lookup[0];
		
		double numerator = Double.parseDouble(line[1]);
		double denominator = keyLookup.getLookupDenominator(lookupKey);
		
		double probability = keyLookup.calculateTransistionProbability(numerator, denominator);
		
		mapKey.set(line[0]);
		mapValue.set(probability);
		
		context.write(mapKey, mapValue);
	}
	
}
