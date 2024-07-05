package server.validator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import server.databaseElements.Attribute;
import server.databaseElements.Database;
import server.databaseElements.Index;
import server.databaseElements.IndexAttribute;
import server.databaseElements.PKAttribute;
import server.databaseElements.Storage;
import server.databaseElements.Table;
import server.databaseElements.UQAttribute;
import server.databaseElements.UniqueKey;
import server.types.Date;
import server.types.DateTime;
import server.validator.commands.AttributeHandler;
import server.validator.commands.DatabaseHandler;
import server.validator.commands.DeleteHandler;
import server.validator.commands.IndexHandler;
import server.validator.commands.InsertHandler;
import server.validator.commands.SelectHandler;
import server.validator.commands.TableHandler;

public class ValidatorDictionary {
	public static final String ok = "Commands executed successfully.";

	private boolean distinct;
	private Database activeDb = null;
	private MongoDatabase activeMongoDb = null;
	private Table activeTable = null;
	private MongoCollection<Document> activeMongoTable = null;
	private Storage data;
	private List<Attribute> attr;
	private Map<String, String> tableMap;
	private List<String> selectParts;
	private Map<String, String> allDocuments;
	private List<TreeMap<String, List<String>>> ownPKtoJointPK;
	
	public ValidatorDictionary(Storage data) {
		this.data = data;
	}

	public String executeRow(MongoClient mongoClient, ClientSession session, String row) {
		String[] initParts = row.split(",");
		String[] parts = partsHotfix(initParts);

//		System.out.println(row);

		if (parts.length == 0) {
			return ok;
		}
		while (parts[0].isEmpty()) {
			parts = offsetArray(parts);
			if (parts.length == 0) {
				return ok;
			}
		}
		for (int i = 0; i < parts.length; i++) {
			System.out.print(parts[i] + ",");
		}
		System.out.println("");


		try {
			if (parts.length > 0) {
//				if (activeDb == null) {
//					System.out.println("null");
//				} else {
//					System.out.println(activeDb.getDbName());
//				}
				switch (parts[0]) {
					case "CREATE DATABASE ": {
						// return createDatabase(mongoClient, session, parts);
						return DatabaseHandler.createDatabase(this, mongoClient, session, parts, activeDb, data,
								activeMongoDb);
					}
					case "DROP DATABASE ": {
						return DatabaseHandler.dropDatabase(this, mongoClient, session, parts, activeDb, data);
					}
					case "USE ": {
						return DatabaseHandler.useDatabase(this, mongoClient, session, parts, activeDb, data,
								activeMongoDb);
					}
					case "CREATE TABLE ": {
						// return createTable(mongoClient, session, parts);
						return TableHandler.createTable(this, mongoClient, session, parts, activeDb, activeTable,
								activeMongoDb, activeMongoTable);
					}
					case "TABLE_CREATED)": {
						// return tableCreated(mongoClient, session, parts);
						return TableHandler.tableCreated(this, mongoClient, session, parts, activeTable);
					}
					case "DROP TABLE ": {
						return TableHandler.dropTable(this, mongoClient, session, parts, activeDb, activeTable,
								activeMongoDb);
					}
					case "CUSTOM_FIELD": {
						return AttributeHandler.addCustomField(this, mongoClient, session, parts, activeTable);
					}
					case "CONSTRAINT ": {
						return AttributeHandler.addConstraint(mongoClient, session, parts, activeMongoDb, activeDb, activeTable);
					}
					case "INDEX ": {
						// return addIndex(mongoClient, session, parts);
						return IndexHandler.addIndex(mongoClient, session, parts, activeTable, activeMongoDb);
					}
					case "UNIQUE INDEX ": {
						// return addUniqueIndex(mongoClient, session, parts);
						return IndexHandler.addUniqueIndex(this, mongoClient, session, parts, activeTable,
								activeMongoDb);
					}
					case "CREATE INDEX ": {
						// return createIndex(mongoClient, session, parts);
						return IndexHandler.createIndex(this, mongoClient, session, parts, activeTable, activeDb);
					}
					case "CREATE UNIQUE INDEX ": {
						// return createUniqueIndex(mongoClient, session, parts);
						return IndexHandler.createUniqueIndex(this, mongoClient, session, parts, activeTable, activeDb);
					}
					case "INSERT INTO ": {
						// return insertInto(mongoClient, session, parts);
						return InsertHandler.insertInto(this, mongoClient, session, parts, activeTable, activeDb,
								activeMongoDb, activeMongoTable, attr);
					}
					case "VALUES": {
						return ok;
					}
					case "CUSTOM_VALUES": {
						// return insertFields(mongoClient, session, parts);
						return InsertHandler.insertFields(this, mongoClient, session, parts, activeTable, activeDb, activeMongoDb,
								activeMongoTable, attr);
					}
					case "DELETE FROM ": {
						return DeleteHandler.deleteFrom(parts, session, activeDb, activeMongoDb, activeMongoTable);
					}
					
					case "SELECT ": {
						return SelectHandler.selectFrom(this, session, parts, activeDb, activeMongoDb);
					}
					
					case "SELECT DISTINCT ": {
						return SelectHandler.selectFrom(this, session, parts, activeDb, activeMongoDb);
					}
					
					case "FROM_TABLE_AS ": {
						return ok;
					}
					case "FROM_TABLE_ABBREVIATION ": {
						return SelectHandler.fromAbbreviation(parts, activeTable, tableMap);
					}
					
					case "JOIN ": {
						return SelectHandler.joinOn(this, session, activeMongoDb, parts, activeDb, activeTable, allDocuments, tableMap);
					}
					
					case "WHERE ": {
						return SelectHandler.whereCondition(this, parts, activeMongoDb, activeDb, activeTable, allDocuments, tableMap);
					}
					
					case "WHERE_AND ": {
						return SelectHandler.whereAnd(this, parts, activeMongoDb, activeDb, activeTable, allDocuments, ownPKtoJointPK, tableMap);
					}
					
					case "GROUP BY ": {
						return SelectHandler.groupBy(this, parts, tableMap, activeDb, activeTable, allDocuments, distinct);
					}
					
					case "GROUP_BY_ENDED ": {
						return ok;
					}
					
					case "SELECT_ENDED ": {
						return SelectHandler.executeSelect(this, activeTable, activeDb, allDocuments, tableMap, distinct);
					}
					default: {
//						return ok;
						return "ValidatorDictionary: Invalid command syntax!";
					}
				}
			}
		} catch (MongoWriteException we) {
			return "Error: Cannot write duplicate Primary Key!";
		} catch (MongoException e) {
			return e.toString();
		}

		return ok;
	}

	// GETTERS/SETTERS
	public List<TreeMap<String, List<String>>> getPKmap() {
		return ownPKtoJointPK;
	}
	
	public void setActiveDb(Database newDb) {
		activeDb = newDb;
	}

	public void setActiveMongoDb(MongoDatabase newDb) {
		activeMongoDb = newDb;
	}

	public void setActiveTable(Table newTable) {
		activeTable = newTable;
	}

	public void setActiveMongoTable(MongoCollection<Document> newTable) {
		activeMongoTable = newTable;
	}

	public void setAttr(List<Attribute> newAttr) {
		attr = newAttr;
	}
	
	public List<String> getSelectParts() {
		return selectParts;
	}
	
	public void setSelectParts(List<String> selectParts) {
		this.selectParts = selectParts;
	}
	
	public void setTableMap(Map<String, String> tm) {
		tableMap = tm;
	}
	
	public void setAllDocuments(Map<String, String> allDocuments) {
		this.allDocuments = allDocuments;
	}
	
	public void setOwnPKtoJointPK(List<TreeMap<String, List<String>>> ownPKtoJointPK) {
		this.ownPKtoJointPK = ownPKtoJointPK;
	}
	
	public void setDistinct(boolean distinct) {
		this.distinct = distinct;
	}
	
	// PRIVATES
	private static boolean isInputString(String str) {
		if (str.length() < 2) {
			return false;
		}
		return (str.charAt(0) == '"' && str.charAt(str.length() - 1) == '"');
	}

	private String[] offsetArray(String[] s) {
		String[] ret = new String[s.length - 1];
		for (int i = 1; i < s.length; i++) {
			ret[i - 1] = s[i];
		}
		return ret;
	}

	public static int parseAttribute(Document newRecord, Attribute nextAttr, String value) {
		if (value.equals("null")) {
			newRecord.put(nextAttr.getAttributeName(), null);
			return 0;
		}
		try {
			switch (nextAttr.getAttributeType()) {
				case "bool": {
					boolean f;
					if (value.equals("true") || value.equals("1")) {
						f = true;
					} else if (value.equals("false") || value.equals("0")) {
						f = false;
					} else {
						return 1;
					}
					newRecord.put(nextAttr.getAttributeName(), f);
					break;
				}
				case "int": {
					newRecord.put(nextAttr.getAttributeName(), Integer.parseInt(value));
					break;
				}
				case "float": {
					newRecord.put(nextAttr.getAttributeName(), Float.parseFloat(value));
					break;
				}
				case "double": {
					newRecord.put(nextAttr.getAttributeName(), Double.parseDouble(value));
					break;
				}
				case "char": {
					String newValue = value;
					if (!isInputString(newValue)) {
						return 1;
					}
					newValue = newValue.substring(1, newValue.length() - 1);
					int maxLength = Integer.parseInt(nextAttr.getAttributeLength());
					if (newValue.length() > maxLength) {
						newValue = newValue.substring(0, maxLength);
					}
					newValue = "\"" + newValue + "\"";
					newRecord.put(nextAttr.getAttributeName(), newValue);
					break;
				}
				case "varchar": {
					String newValue = value;
					if (!isInputString(newValue)) {
						return 1;
					}
					newValue = newValue.substring(1, newValue.length() - 1);
					int maxLength = Integer.parseInt(nextAttr.getAttributeLength());
					if (newValue.length() > maxLength) {
						newValue = newValue.substring(0, maxLength);
					}
					newValue = "\"" + newValue + "\"";
					newRecord.put(nextAttr.getAttributeName(), newValue);
					break;
				}
				case "datetime": {
					if (!isInputString(value)) {
						return 1;
					}
					DateTime newValue = new DateTime(value.substring(1, value.length() - 1));
					if (newValue.isValid()) {
						newRecord.put(nextAttr.getAttributeName(), newValue.getDateTime());
					} else {
						return 1;
					}
					break;
				}
				case "date": {
					if (!isInputString(value)) {
						return 1;
					}
					Date newValue = new Date(value.substring(1, value.length() - 1));
					if (newValue.isValid()) {
						newRecord.put(nextAttr.getAttributeName(), newValue.getDate());
					} else {
						return 1;
					}
					break;
				}
				default:
					return 1;
			}
			return 0;
		} catch (NumberFormatException e) {
			System.out.println(e);
			return 1;
		} catch (MongoWriteException e) {
			System.out.println(e);
			return 1;
		}
	}

	public boolean isUniqueCombination(String[] parts) throws MongoException {
		ArrayList<String> fields = new ArrayList<>();
		for (int i = 3; i < parts.length - 1; i += 3) {
			fields.add(parts[i]);
		}

		List<String> chosenParts = new ArrayList<>();

		return backtrackUqSearch(1, fields.size(), fields, chosenParts);
	}

	private boolean isUniquePart(List<String> chosenParts) throws MongoException {
		if (chosenParts.isEmpty()) {
			return false;
		}

		List<PKAttribute> pks = activeTable.getPrimaryKeys(); // contains fields; getPKAttributeName()
		List<UniqueKey> uqs = activeTable.getUniqueKeys(); // contains UKs, which contain fields; uqAttr.getName()

		// check if chosen part is PK itself
		if (pks != null && pks.size() == chosenParts.size()) {
			boolean[] appeared = new boolean[chosenParts.size()];
			for (PKAttribute pkAttr : pks) {
				for (int i = 0; i < chosenParts.size(); i++) {
					if (chosenParts.get(i).equals(pkAttr.getPKAttributeName())) {
						appeared[i] = true;
						break;
					}
				}
			}

			// check if entire chosenPart appeared
			boolean ok = true;
			for (int i = 0; i < chosenParts.size(); i++) {
				if (!appeared[i]) {
					ok = false;
				}
			}
			if (ok) {
				return true;
			}
		}

		List<UQAttribute> uqAttrs = new ArrayList<>();
		// check if chosen part is one complete UK
		if (uqs != null) {
			for (UniqueKey uq : uqs) {
				uqAttrs = uq.getUqAttr(); // list of attributes
				if (uqAttrs.size() == chosenParts.size()) {
					boolean[] appeared = new boolean[chosenParts.size()];
					for (UQAttribute uqa : uqAttrs) {
						for (int i = 0; i < chosenParts.size(); i++) {
							if (chosenParts.get(i).equals(uqa.getName())) {
								appeared[i] = true;
								break;
							}
						}
					}

					// check if entire chosenPart appeared
					boolean ok = true;
					for (int i = 0; i < chosenParts.size(); i++) {
						if (!appeared[i]) {
							ok = false;
						}
					}
					if (ok) {
						return true;
					}
				}
			}
		}
		return false;
	}

	private boolean backtrackUqSearch(int lev, int maxDepth, List<String> fields, List<String> chosenParts)
			throws MongoException {
		if (lev > maxDepth) {
			return false;
		}
		int ind = lev - 1;

		for (int i = 0; i <= 1; i++) {
			if (i == 1) {
				chosenParts.add(fields.get(ind));
				if (isUniquePart(chosenParts)) {
					return true;
				}
			}

			if (backtrackUqSearch(lev + 1, maxDepth, fields, chosenParts)) {
				return true;
			}
			if (i == 1) {
				chosenParts.remove(fields.get(ind));
			}
		}
		return false;
	}

	public static String[] partsHotfix(String[] initParts) {
		int initLength = initParts.length;
		String[] processedParts = new String[initLength];
		int newLength = 0, i = 0;
		boolean needsClosingQuote = false;
		String partBuilder = "";
		while (i < initLength) {
			if (initParts[i].equals("\"")) {
				if (needsClosingQuote) {
					partBuilder = partBuilder + initParts[i]; // add this final part to it
					// add to finalized
					processedParts[newLength] = partBuilder;
					newLength++;

					needsClosingQuote = false;
					partBuilder = "";
				} else {
					needsClosingQuote = true;
					partBuilder = initParts[i] + ",";
				}
				i++;
				continue;
			}

			if (initParts[i].startsWith("\"") && initParts[i].charAt(initParts[i].length() - 1) == '\"') {
				// ENTIRE input string
				// add to finalized
				processedParts[newLength] = initParts[i];
				newLength++;
			} else if (initParts[i].startsWith("\"")) {
				// only START of an input string
				needsClosingQuote = true;
				partBuilder = initParts[i] + ",";
			} else if (!initParts[i].isEmpty() && initParts[i].charAt(initParts[i].length() - 1) == '\"') {
				// only END of an input string
				partBuilder = partBuilder + initParts[i]; // add this final part to it
				// add to finalized
				processedParts[newLength] = partBuilder;
				newLength++;

				needsClosingQuote = false;
				partBuilder = "";
			} else if (needsClosingQuote) {
				// only MIDDLE of an input string
				partBuilder = partBuilder + initParts[i] + ",";
			} else {
				// just a simple input, not string
				// add to finalized
				processedParts[newLength] = initParts[i];
				newLength++;
			}

			i++;
		}
		String[] finalParts = new String[newLength];
		System.arraycopy(processedParts, 0, finalParts, 0, newLength);
		return finalParts;
	}

	public static Document convertDocument(Document doc, Table activeTable) {
		Document pkDoc;
		boolean hasPK;
		try {
			pkDoc = (Document) doc.get("_id");
			hasPK = true;
//			System.out.println(pkDoc);
		} catch (ClassCastException e) {
			pkDoc = new Document();
			hasPK = false;
		}
		if (pkDoc == null) {
			pkDoc = new Document();
			hasPK = false;
		}
		StringBuilder PK = new StringBuilder("");
		StringBuilder value = new StringBuilder("");
		
		for (Attribute attr : activeTable.getAttributes()) {
			if (attr.isPK(activeTable)) {
				PK.append(pkDoc.get(attr.getAttributeName()));
				PK.append(",");
			}
			else {
				value.append(doc.get(attr.getAttributeName()));
				value.append(",");
			}
		}
		
		/*
		for (String pkKey : pkDoc.keySet()) {
			PK.append(pkDoc.get(pkKey));
			PK.append(",");
		}
		
		Set<String> valueSet = doc.keySet();
		if (valueSet != null) {
			for (String valKey : valueSet) {
				if (!valKey.equals("_id")) {
					value.append(doc.get(valKey));
					value.append(",");
				}
			}
		}
		*/
		
		Document retVal = new Document();
		if (hasPK) {
			String fullPK = PK.toString();
			retVal.put("_id", fullPK.substring(0, fullPK.length() - 1));
		}
		
		String fullValue = value.toString();
		int stop = Math.max(fullValue.length() - 1, 0);
		retVal.append("value", fullValue.substring(0, stop));
		
		return retVal;
	}
	
	public static Document convertDocument(Document doc, Table activeTable, Index activeIndex) {
		Document pkDoc;
		boolean hasPK;
		try {
			pkDoc = (Document) doc.get("_id");
			hasPK = true;
//			System.out.println(pkDoc);
		} catch (ClassCastException e) {
			pkDoc = new Document();
			hasPK = false;
		}
		if (pkDoc == null) {
			pkDoc = new Document();
			hasPK = false;
		}
		StringBuilder PK = new StringBuilder("");
		StringBuilder value = new StringBuilder("");
		
		for (IndexAttribute attr : activeIndex.getIndexes()) {
			/*if (attr.isPK(activeTable)) {
				PK.append(pkDoc.get(attr.getAttributeName()));
				PK.append(",");
			}
			else {
				value.append(doc.get(attr.getAttributeName()));
				value.append(",");
			}*/
			PK.append(pkDoc.get(attr.getAttr()));
			PK.append(",");
		}
		
		for (PKAttribute pkAttr : activeTable.getPrimaryKeys()) {
			if (pkDoc.get(pkAttr.getPKAttributeName()) != null) {
				value.append(pkDoc.get(pkAttr.getPKAttributeName()));
			} else {
				value.append(doc.get(pkAttr.getPKAttributeName()));
			}
			value.append(",");
		}
		
		/*
		for (String pkKey : pkDoc.keySet()) {
			PK.append(pkDoc.get(pkKey));
			PK.append(",");
		}
		
		Set<String> valueSet = doc.keySet();
		if (valueSet != null) {
			for (String valKey : valueSet) {
				if (!valKey.equals("_id")) {
					value.append(doc.get(valKey));
					value.append(",");
				}
			}
		}
		*/
		
		Document retVal = new Document();
		if (hasPK) {
			String fullPK = PK.toString();
			retVal.put("_id", fullPK.substring(0, fullPK.length() - 1));
		}
		
		String fullValue = value.toString();
		int stop = Math.max(fullValue.length() - 1, 0);
		retVal.append("value", fullValue.substring(0, stop));
		
		return retVal;
	}
}
