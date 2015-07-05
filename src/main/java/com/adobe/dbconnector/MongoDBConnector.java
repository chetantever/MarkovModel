package com.adobe.dbconnector;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDBConnector {

	private String hostname;
	private int port;
	private String dbName;
	private DBCollection collection;
	private MongoClient mongo;
	private BasicDBObject doc;
	
	private static MongoDBConnector instance = null;
		
	public MongoDBConnector() {
		// TODO Auto-generated constructor stub
	}

	//Parameterized constructor
	public MongoDBConnector(String hostname, int port, String dbName,String collectionName) {
		// TODO Auto-generated constructor stub
		try {
			this.hostname = hostname;
			this.port = port;
			this.dbName = dbName;
			this.collection = createCollection(collectionName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());
		}
	}
	
	//Create collection
	protected DBCollection createCollection(String collectionName) {
		DBCollection table = null;
		try {
			mongo = new MongoClient(hostname, port);
			DB db = mongo.getDB(dbName);
			table = db.getCollection(collectionName);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println(e.getMessage());
		}
		return table;
	}
	
	//Insert new ID
	public void insertID(String value) {
		doc = new BasicDBObject("_id", value);
	    collection.insert(doc);
	}
	
	//Search for ID
	public DBObject findDocumentById(String id) {
	    BasicDBObject query = new BasicDBObject();
	    query.put("_id", id);
	    DBObject dbObj = collection.findOne(query);
	    return dbObj;
	}
	
	//Update collection
	public void updateQuery(String id,String key,String value) {
		BasicDBObject newDocument = new BasicDBObject();
		BasicDBObject searchQuery = new BasicDBObject().append("_id", id);
		newDocument.append("$set", new BasicDBObject().append(key, value));
		collection.update(searchQuery, newDocument);
	}
	
	//Update collection
		public void updateQuery(String id,String key,double value) {
			BasicDBObject newDocument = new BasicDBObject();
			BasicDBObject searchQuery = new BasicDBObject().append("_id", id);
			newDocument.append("$set", new BasicDBObject().append(key, value));
			collection.update(searchQuery, newDocument);
		}
	
	//Deleted document by ID
	public void deleteByID(String id) {
		BasicDBObject deleteQuery = new BasicDBObject();
		deleteQuery.put("_id", id);
		DBCursor cursor = collection.find(deleteQuery);
		while (cursor.hasNext()) {
		    DBObject item = cursor.next();
		    collection.remove(item);
		}
	}
	
	//Drop a collection
	public void dropCollection() {
		collection.drop();
	}
}
