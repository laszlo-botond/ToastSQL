package server.validator.commands;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;

import server.databaseElements.Attribute;
import server.databaseElements.Database;
import server.databaseElements.FKAttribute;
import server.databaseElements.ForeignKey;
import server.databaseElements.Index;
import server.databaseElements.IndexAttribute;
import server.databaseElements.PKAttribute;
import server.databaseElements.Table;
import server.validator.ValidatorDictionary;

public class InsertHandler {
	public static String insertInto(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Table activeTable, Database activeDb, MongoDatabase activeMongoDb, MongoCollection<Document> activeMongoTable, List<Attribute> attr) throws MongoException {
		if (activeDb == null) {
			return "Error: No active Database specified!";
		}
		activeTable = activeDb.getTable(parts[1]);
		valDict.setActiveTable(activeTable);
		if (activeTable == null) {
			return "Error: can't find Table \"" + parts[1] + "\" in Database \"" + activeDb.getDbName() + "\"!";
		}
		activeMongoTable = activeMongoDb.getCollection(parts[1]);
		valDict.setActiveMongoTable(activeMongoTable);

		attr = new ArrayList<Attribute>();
		for (int i = 3; i < parts.length - 1; i += 3) {
			Attribute tmp = activeTable.getAttribute(parts[i]);
			if (tmp == null) {
				return "Error: can't find Attribute \"" + parts[i] + "\" in Table \"" + parts[1] + "\"!";
			}
			attr.add(tmp);
		}
		valDict.setAttr(attr);

		return ValidatorDictionary.ok;
	}

	public static String insertFields(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Table activeTable, Database activeDb, MongoDatabase activeMongoDb, MongoCollection<Document> activeMongoTable, List<Attribute> attr) throws MongoException {
		Document newPK = new Document();
		Document newRecord = new Document();
		
		List<PKAttribute> pkAttrs = activeTable.getPrimaryKeys();
		
		if (pkAttrs != null) { 
			for (PKAttribute pkAttr : pkAttrs) {
				newPK.append(pkAttr.getPKAttributeName(), null);
			}
			for (Attribute notPkAttr : activeTable.getAttributes()) {
				if (!notPkAttr.isPK(activeTable)) {
					newRecord.append(notPkAttr.getAttributeName(), null);
				}
			}
		}
		
		
		ListIterator<Attribute> itr = attr.listIterator();
		for (int i = 2; i < parts.length - 1; i += 3) {
			
			if (!itr.hasNext()) {
				return "Error: more values than fields given to INSERT command!";
			}

			Attribute nextAttr = itr.next();
			if (nextAttr.isPK(activeTable)) {
				// Add PK values to PK Document
				if (valDict.parseAttribute(newPK, nextAttr, parts[i]) == 1) {
					return "Type mismatch: can't assign value \"" + parts[i] + "\" to Attribute \"" + nextAttr.getAttributeName() + "\"!";
				}
			}
			else {
				// Add non-PK values to Record Document
				if (valDict.parseAttribute(newRecord, nextAttr, parts[i]) == 1) {
					return "Type mismatch: can't assign value \"" + parts[i] + "\" to Attribute \"" + nextAttr.getAttributeName() + "\"!";
				}
			}

		}

		
		if (pkAttrs != null) {
			List<String> pkNames = pkAttrs.stream().map(pk -> pk.getPKAttributeName()).toList();
			for (String pk : pkNames) {
				// If there's PK field with null value, error
				if (newPK.get(pk) == null) {
					return "Error: can't insert record into Table \"" + activeTable.getTableName() + "\" with Primary Key attribute set to null!";
				}
			}
		}

		for (Attribute a : activeTable.getAttributes()) {
			if (a.isPK(activeTable)) {
				continue;
			}
			if (newRecord.get(a.getAttributeName()) == null) {
				if (a.getDefault() == null) {
					if (a.isNullable()) {
						newRecord.put(a.getAttributeName(), null);
					// If there's UQ field with null value, error
					} else {
						return "Error: can't insert record into Table \"" + activeTable.getTableName() + "\" with NOT NULLABLE attribute set to null!";
					}
				} else {
					if (valDict.parseAttribute(newRecord, a, a.getDefault()) == 1) {
						return "Type mismatch: can't assign value \"" + a.getDefault() + "\" to Attribute \"" + a.getAttributeName() + "\"!";
					}
				}
			}
		}

		if (activeTable.getForeignKeys() != null) {
			for (ForeignKey activeFK : activeTable.getForeignKeys()) {
				Table refTable = activeDb.getTable(activeFK.getReferencedTable());
				if (refTable == null) {
					return "Error: Invalid foreign key!";
				}
				String toFind = "";
				for (Attribute refAttr : refTable.getAttributes()) {
					if (refAttr.isPK(refTable)) {
						for (FKAttribute fkAttr : activeFK.getFKAttributes()) {
							if (fkAttr.getReference().getRefAttr().equals(refAttr.getAttributeName())) {
	//							System.out.println(fkAttr.getFKAttributeName() + " -> " + refAttr.getAttributeName());
								if (activeTable.getAttribute(fkAttr.getFKAttributeName()).isPK(activeTable)) {
									toFind = toFind + newPK.get(fkAttr.getFKAttributeName()) + ",";
								} else {
									toFind = toFind + newRecord.get(fkAttr.getFKAttributeName()) + ",";
								}
							}
						}
					}
				}
//				System.out.println("toFind: " + toFind.substring(0, toFind.length() - 1));
				boolean found = false;
				try {
					FindIterable<Document> matchingFK = activeMongoDb.getCollection(refTable.getTableName()).find(session, eq("_id", toFind.substring(0, toFind.length() - 1)));
					for (Document doc : matchingFK) {
						found = true;
					}
				} catch (NullPointerException | MongoException e) {
					found = false;
				}
				if (!found) {
					return "Error! Insert statement failed Referential Integrity Check!";
				}
			}
		}
		
		try {
			if (!newPK.isEmpty()) {
				newRecord.append("_id", newPK);
			}
			activeMongoTable.insertOne(session, ValidatorDictionary.convertDocument(newRecord, activeTable));
			
			// Start building index record
			List<Index> indexList = activeTable.getIndexFiles();
			if (indexList != null) {
				for (Index ind : indexList) {
					MongoCollection<Document> activeMongoIndex = activeMongoDb.getCollection(ind.getIndexName());
					if (ind.isIndexUnique()) {
						insertIntoUQIndex(session, ind, newPK, newRecord, activeMongoIndex, activeTable);
					}
					else {
						insertIntoCommonIndex(session, ind, newPK, newRecord, activeMongoIndex, activeTable);
					}
					
					
				}
			}
			
		} catch (MongoWriteException e) {
			return "Error: Duplicate value on UNIQUE attribute!";
		}
		return ValidatorDictionary.ok;
	}

	public static void insertIntoUQIndex(ClientSession session, Index ind, Document newPK, Document newRecord, MongoCollection<Document> activeMongoIndex, Table activeTable) throws MongoWriteException {
		Document indexPK = new Document();
		Document indexRecord = new Document();
		boolean isIndexPK;

//		System.out.println("INSERT INTO UQ INDEX NOW");
		
		List<IndexAttribute> indAttrList = ind.getIndexes();
		List<PKAttribute> pkAttrList = activeTable.getPrimaryKeys();
		if (indAttrList != null) {
			for (IndexAttribute indAttr : indAttrList) {
				isIndexPK = false;
				if (pkAttrList != null) {
					for (PKAttribute pkAttr : pkAttrList) {
						if (pkAttr.getPKAttributeName().equals(indAttr.getAttr())) {
							isIndexPK = true;
							break;
						}
					}
				}
				// Table PK
				if (isIndexPK) {
					indexPK.append(indAttr.getAttr(), newPK.get(indAttr.getAttr()));
//					System.out.println(newPK.get(indAttr.getAttr()));
				}
				// Table non-PK
				else {
					indexPK.append(indAttr.getAttr(), newRecord.get(indAttr.getAttr()));
//					System.out.println(newRecord.get(indAttr.getAttr()));
				}
			}
		}
		indexRecord.append("_id", indexPK);
		if (pkAttrList != null) {
			for (PKAttribute pkAttr : pkAttrList) {
				indexRecord.append(pkAttr.getPKAttributeName(), newPK.get(pkAttr.getPKAttributeName()));
			}
		}
//		System.out.println("IRID: " + indexRecord);
//		System.out.println("ID: " + ValidatorDictionary.convertDocument(indexRecord, activeTable, ind));
//		System.out.println("Value: " + ValidatorDictionary.convertDocument(indexRecord, activeTable, ind).get("value"));
		activeMongoIndex.insertOne(session, ValidatorDictionary.convertDocument(indexRecord, activeTable, ind));
	}
	
	public static void insertIntoCommonIndex(ClientSession session, Index ind, Document newPK, Document newRecord, MongoCollection<Document> activeMongoIndex, Table activeTable) throws MongoWriteException {
		
		StringBuilder indexPK = new StringBuilder("");
		StringBuilder indexRecord = new StringBuilder("");
		Document wrapperRecord = new Document();
		boolean isIndexPK;
		List<PKAttribute> pkAttrList = activeTable.getPrimaryKeys();
		List<IndexAttribute> indAttrList = ind.getIndexes();
		if (indAttrList != null ) {
			for (IndexAttribute indAttr : indAttrList) {
				isIndexPK = false;
				
				if (pkAttrList != null) {
					for (PKAttribute pkAttr : pkAttrList) {
						if (pkAttr.getPKAttributeName().equals(indAttr.getAttr())) {
							isIndexPK = true;
							break;
						}
					}
				}
				// Table PK
				if (isIndexPK) {
					indexPK.append(newPK.get(indAttr.getAttr()));
					indexPK.append(",");
				}
				// Table non-PK
				else {
					indexPK.append(newRecord.get(indAttr.getAttr()));
					indexPK.append(",");
				}
			}
		}
		
		if (pkAttrList != null) {
			for (PKAttribute pkAttr : pkAttrList) {
				indexRecord.append(newPK.get(pkAttr.getPKAttributeName()));
				indexRecord.append(",");
			}
		}
		
		String fullIndexPK = indexPK.toString();
		String fullIndexRecord = indexRecord.toString();
		fullIndexPK = fullIndexPK.substring(0, fullIndexPK.length() - 1);
		fullIndexRecord = fullIndexRecord.substring(0, fullIndexRecord.length() - 1);
		
		// Find the row of new entry in index
		FindIterable<Document> place = activeMongoIndex.find(session, eq("_id", fullIndexPK));
		MongoCursor<Document> indPlace = place.iterator();
		
		// If empty, create list, add one element, append
		if (!indPlace.hasNext()) {
			wrapperRecord.append("_id", fullIndexPK);
			wrapperRecord.append("value", fullIndexRecord);
			
			activeMongoIndex.insertOne(session, wrapperRecord);
		}
		// Else, get existing list, add one element, append
		else {
			String oldValue = (String) indPlace.next().get("value");
			fullIndexRecord = oldValue + "\n" + fullIndexRecord;
			activeMongoIndex.updateOne(session, eq("_id", fullIndexPK), Updates.set("value", fullIndexRecord));
		}
	}
}
