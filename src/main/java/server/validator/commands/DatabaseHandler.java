package server.validator.commands;

import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import server.databaseElements.Database;
import server.databaseElements.Storage;
import server.validator.ValidatorDictionary;

public class DatabaseHandler {
	public static String createDatabase(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Database activeDb, Storage data, MongoDatabase activeMongoDb) throws MongoException {
		activeDb = new Database(parts[1]);
		
		if (data.addDatabase(activeDb) == 1) {
			return "Database \"" + parts[1] + "\" already exists!";
		}
		valDict.setActiveDb(activeDb);

		activeMongoDb = mongoClient.getDatabase(parts[1]);
		valDict.setActiveMongoDb(activeMongoDb);
		return ValidatorDictionary.ok;
	}

	public static String dropDatabase(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Database activeDb, Storage data) throws MongoException {
		Database tmp = data.getDatabase(parts[1]);
		if (activeDb == tmp) {
			activeDb = null;
		}
		if (data.removeDatabase(tmp) == 1) {
			return "Couldn't drop database \"" + parts[1] + "\"!";
		}
		mongoClient.getDatabase(parts[1]).drop();
		valDict.setActiveDb(null);
		valDict.setActiveMongoDb(null);
		return ValidatorDictionary.ok;
	}

	public static String useDatabase(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Database activeDb, Storage data, MongoDatabase activeMongoDb) throws MongoException {
		activeDb = data.getDatabase(parts[1]);
		
		if (activeDb == null)
			return "Error: can't find Database \"" + parts[1] + "\"!";
		valDict.setActiveDb(activeDb);
		
		activeMongoDb = mongoClient.getDatabase(parts[1]);
		valDict.setActiveMongoDb(activeMongoDb);
		
		return ValidatorDictionary.ok;
	}
}
