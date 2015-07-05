package com.adobe.dbconnector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class UploadTransitionMatrix {

	private static final String ROW_DELIM = "\t"; 
	private static final String KEY_DELIM = ","; 
	
	static MongoDBConnector mongoDbObject ;
	
	public static void main(String[] args) throws IOException {
		mongoDbObject = new MongoDBConnector("192.168.56.200", 27017, "PM", "TransitionProbabilityMatrix");
		BufferedReader br = new BufferedReader(new FileReader("PM-Result-out-part-m-00000.tsv"));
		String line;
		while((line=br.readLine())!=null) {
			
			String[] array = line.split(ROW_DELIM);
			String[] key = array[0].split(KEY_DELIM);
			
			String id = key[0];
			if((mongoDbObject.findDocumentById(id))==null) {
				mongoDbObject.insertID(id);
			}
			mongoDbObject.updateQuery(id, key[1], array[1]);
			
		}
		System.out.println(mongoDbObject.findDocumentById("AirConditioner"));
	}
}
