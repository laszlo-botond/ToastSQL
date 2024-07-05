package server.validator.commands;

import java.util.List;

import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import server.databaseElements.Database;
import server.databaseElements.Index;
import server.databaseElements.Table;
import server.databaseElements.UniqueKey;
import server.validator.ValidatorDictionary;

public class TableHandler {
	public static String createTable(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Database activeDb, Table activeTable, MongoDatabase activeMongoDb, MongoCollection<Document> activeMongoTable) throws MongoException {
		if (activeDb == null) {
			return "Error: no active database specified!";
		}
		activeTable = new Table(parts[1]);
		valDict.setActiveTable(activeTable);
		
		if (activeDb.addTable(activeTable) == 1) {
			return "Table \"" + parts[1] + "\" already exists!";
		}
		activeMongoDb.createCollection(session, parts[1]);
		activeMongoTable = activeMongoDb.getCollection(parts[1]);
		valDict.setActiveMongoTable(activeMongoTable);

		return ValidatorDictionary.ok;
	}

	public static String tableCreated(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Table activeTable) throws MongoException {
		try {
			List<UniqueKey> uqs = activeTable.getUniqueKeys();
			if (uqs != null) {
				for (UniqueKey uq : uqs) {
					List<String> indexFields = uq.getUqAttr().stream().map(attr -> attr.getName()).toList();
					String newCommand = "UNIQUE INDEX ," + activeTable.generateUQName() + ",(,";
					if (indexFields != null) {
						for (String indField : indexFields) {
							newCommand += indField + ",,,";
						}
					}
					newCommand = newCommand.substring(0, newCommand.length() - 2) + ")";
					String response = valDict.executeRow(mongoClient, session, newCommand);
					if (!response.equals(ValidatorDictionary.ok)) {
						return response;
					}
					
				}
			}
			return ValidatorDictionary.ok;
		} catch (MongoWriteException e) {
			return "Error: " + e;
		}
	}
	
	public static String dropTable(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Database activeDb, Table activeTable, MongoDatabase activeMongoDb) throws MongoException {
		activeTable = activeDb.getTable(parts[1]);
		valDict.setActiveTable(activeTable);
		if (activeDb == null || activeTable == null) {
			return "Error: couldn't drop table \"" + parts[1] + "\"!";
		}		
		
		if (activeTable.isTableReferenced(activeDb)) {
			return "Error: Dropping Table " + activeTable.getTableName() + " violates Referential Integrity Check!";
		}
		
		
		List<Index> indexFiles = activeTable.getIndexFiles();
		if (indexFiles != null) {
			for (Index indColl : indexFiles) {
				activeMongoDb.getCollection(indColl.getIndexName()).drop();
			}
			
		}
		if (activeDb.removeTable(activeDb.getTable(parts[1])) == 1) {
			return "Error: couldn't drop table \"" + parts[1] + "\"!";
		}
		activeMongoDb.getCollection(parts[1]).drop();
		valDict.setActiveTable(null);
		valDict.setActiveMongoTable(null);
		return ValidatorDictionary.ok;
	}
}
