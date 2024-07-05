package server.validator.commands;

import static com.mongodb.client.model.Filters.eq;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import server.databaseElements.Database;
import server.databaseElements.Index;
import server.databaseElements.IndexAttribute;
import server.databaseElements.Table;
import server.validator.ValidatorDictionary;

public class IndexHandler {
	public static String addIndex(MongoClient mongoClient, ClientSession session, String[] parts, Table activeTable, MongoDatabase activeMongoDb) throws MongoException {
		if (activeTable == null) {
			return "Error: no active table specified!";
		}
		if (activeTable.getPrimaryKeys() == null) {
			return "Error: no primary key to use in Index!";
		}
		if (activeTable.addIndexFile(parts[1], "0") == 1) {
			return "Can't create Index on Table \"" + activeTable.getTableName() + "\"!";
		}
		Index indf = activeTable.getIndexFile(parts[1]);

		for (int i = 3; i < parts.length - 1; i += 3) {
			if (indf.addIndex(activeTable, parts[i]) == 1) {
				return "Can't add attribute \"" + parts[i] + "\" to Index on Table \"" + activeTable.getTableName() + "\"!";
			}
		}

		activeMongoDb.createCollection(session, parts[1]);
		MongoCollection<Document> mongoIndexTable = activeMongoDb.getCollection(parts[1]);
//		System.out.println(mongoIndexTable);
		for (Document oneDoc : activeMongoDb.getCollection(activeTable.getTableName()).find(session)) {
			String oneVal = "";
			for (IndexAttribute indAttr : indf.getIndexes()) {
				List<Integer> posList = activeTable.getAttrPos(indAttr.getAttr());
				if (posList.get(0) == 1) {
					oneVal = oneVal + ValidatorDictionary.partsHotfix(((String) oneDoc.get("_id")).split(","))[posList.get(1)];
				} else {
					oneVal = oneVal + ValidatorDictionary.partsHotfix(((String) oneDoc.get("value")).split(","))[posList.get(1)];
				}
			}
			Document docToInsert = new Document();
			docToInsert.put("_id", oneVal);
			docToInsert.put("value", (String) oneDoc.get("_id"));
//			System.out.println(docToInsert);
			FindIterable<Document> alreadyHas = mongoIndexTable.find(session, eq("_id", oneVal));
			for (Document alreadyDoc : alreadyHas) {
				String oldVal = (String) alreadyDoc.get("value");
				docToInsert.put("value", oldVal + "\n" + (String) oneDoc.get("_id"));
				mongoIndexTable.deleteOne(session, eq("_id", oneVal));
			}
			mongoIndexTable.insertOne(session, docToInsert);
		}
		return ValidatorDictionary.ok;
	}

	public static String addUniqueIndex(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Table activeTable, MongoDatabase activeMongoDb) throws MongoException {
		if (activeTable == null) {
			return "Error: no active table specified!";
		}
		if (activeTable.addIndexFile(parts[1], "1") == 1) {
			return "Can't create Unique Index on Table \"" + activeTable.getTableName() + "\"!";
		}
		Index indf = activeTable.getIndexFile(parts[1]);
		
		
		// new logic
		if (!valDict.isUniqueCombination(parts)) {
			return "Can't create Unique Index on NOT Unique attributes!";
		}
		for (int i = 3; i < parts.length - 1; i += 3) {
			if (indf.addIndex(activeTable, parts[i]) == 1) {
				return "Can't add attribute \"" + parts[i] + "\" to Index on Table \"" + activeTable.getTableName() + "\"!";
			}
		}
		
		activeMongoDb.createCollection(session, parts[1]);
		MongoCollection<Document> mongoIndexTable = activeMongoDb.getCollection(parts[1]);
//		System.out.println(mongoIndexTable);
		for (Document oneDoc : activeMongoDb.getCollection(activeTable.getTableName()).find(session)) {
			String oneVal = "";
			for (IndexAttribute indAttr : indf.getIndexes()) {
				List<Integer> posList = activeTable.getAttrPos(indAttr.getAttr());
				if (posList.get(0) == 1) {
					oneVal = oneVal + ValidatorDictionary.partsHotfix(((String) oneDoc.get("_id")).split(","))[posList.get(1)];
				} else {
					oneVal = oneVal + ValidatorDictionary.partsHotfix(((String) oneDoc.get("value")).split(","))[posList.get(1)];
				}
			}
			FindIterable<Document> alreadyHas = mongoIndexTable.find(session, eq("_id", oneVal));
			for (Document alreadyFound : alreadyHas) {
				return "Error: Duplicate key on UNIQUE attribute(s): \"" + alreadyFound.get("_id") + "\"!";
			}
			Document docToInsert = new Document();
			docToInsert.put("_id", oneVal);
			docToInsert.put("value", (String) oneDoc.get("_id"));
			mongoIndexTable.insertOne(session, docToInsert);
		}
		return ValidatorDictionary.ok;
	}

	public static String createIndex(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Table activeTable, Database activeDb) throws MongoException {
		if (activeDb == null) {
			return "Error: no active database specified!";
		}
		activeTable = activeDb.getTable(parts[3]);
		valDict.setActiveTable(activeTable);
		if (activeTable == null) {
			return "Error: can't find Table \"" + parts[3] + "\" in Database \"" + activeDb.getDbName() + "\"!";
		}
		String newCommand = "INDEX ," + parts[1] + ",";
		for (int i = 4; i < parts.length; i++) {
			newCommand = newCommand + parts[i] + ",";
		}
		return valDict.executeRow(mongoClient, session, newCommand);
	}

	public static String createUniqueIndex(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Table activeTable, Database activeDb) throws MongoException {
		if (activeDb == null) {
			return "Error: no active database specified!";
		}
		activeTable = activeDb.getTable(parts[3]);
		valDict.setActiveTable(activeTable);
		if (activeTable == null) {
			return "Error: can't find Table \"" + parts[3] + "\" in Database \"" + activeDb.getDbName() + "\"!";
		}
		String newCommand = "UNIQUE INDEX ," + parts[1] + ",";
		for (int i = 4; i < parts.length; i++) {
			newCommand = newCommand + parts[i] + ",";
		}
		return valDict.executeRow(mongoClient, session, newCommand.substring(0, newCommand.length() - 1));
	}
}
