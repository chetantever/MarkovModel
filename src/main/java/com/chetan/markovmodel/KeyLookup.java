package com.chetan.markovmodel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

public class KeyLookup {

	private static String ROW_DELIM = "\t";
	private String filePath;
	private HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
	private int lookupValue = 0;
		
	public void setLookupFilePath(String path) {
		filePath = path;
		insertLookupProbability();
	}
	
	@SuppressWarnings("unchecked")
	private void insertLookupProbability() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line;
			while((line=br.readLine())!=null) {
				String[] entry = line.split(ROW_DELIM);
				hashMap.put(entry[0], Integer.parseInt(entry[1]));
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
	}
	
	public int getLookupDenominator(String lookup) {
		lookupValue = hashMap.get(lookup);
		return lookupValue;
	}
	
	public double calculateTransistionProbability(double numerator,double denominator) {
		return (numerator/denominator);
	}
	
}
