package server.validator.commands;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import server.databaseElements.Attribute;
import server.databaseElements.Database;
import server.databaseElements.FKAttribute;
import server.databaseElements.ForeignKey;
import server.databaseElements.Index;
import server.databaseElements.IndexAttribute;
import server.databaseElements.Table;
import server.validator.ValidatorDictionary;

public class AttributeHandler {
	public static String addCustomField(ValidatorDictionary valDict, MongoClient mongoClient, ClientSession session, String[] parts, Table activeTable) throws MongoException {
		if (activeTable == null) {
			return "Error: no active table specified!";
		}
		String name = parts[1];
		String type = parts[2].toLowerCase().trim();
		String length = null;
		String isnull = null;
		String defaultVal = null;

		int nextInd = 3;
		if (type.equals("varchar") || type.equals("char")) {
			length = parts[4];
			nextInd = 6;
		}
		boolean needUQ = false;
		boolean needPK = false;
		while (nextInd < parts.length) {
			switch (parts[nextInd]) {
				case "PRIMARY KEY": {
					needPK = true;
					break;
				}
				case "UNIQUE": {
					needUQ = true;
					break;
				}
				case "NULL": {
					isnull = "1";
					break;
				}
				case "NOT NULL": {
					isnull = "0";
					break;
				}
				case "NOT NULL ": {
					isnull = "0";
					break;
				}
				case "DEFAULT ": {
					defaultVal = parts[nextInd + 1];
					if (ValidatorDictionary.parseAttribute(new Document(), new Attribute("tmp", type, length, null, null), defaultVal) == 1) {
						return "Error: Can't assign value " + defaultVal + "to attribute \"" + name + "\"!";
					}
					break;
				}
			}
			nextInd++;
		}
		if (activeTable.addAttribute(name, type, length, isnull, defaultVal) == 1) {
			return "Attribute \"" + name + "\" already exists in table \"" + activeTable.getTableName() + "\"!";
		}
		if (needPK && activeTable.addToPK(name) == 1) {
			return "Can't add attribute \"" + name + "\" to Primary Key!";
		}
		if (needUQ) {
			String tmp = activeTable.generateUQName();
			if (activeTable.addUQ(tmp) == 1) {
				return "Can't create Unique Key constraint!";
			}
			if (activeTable.addToUQ(tmp, name) == 1) {
				return "Can't add attribute \"" + name + "\" to Unique Key!";
			}
		}
		return ValidatorDictionary.ok;
	}

	public static String addConstraint(MongoClient mongoClient, ClientSession session, String[] parts, MongoDatabase activeMongoDb, Database activeDb, Table activeTable) throws MongoException {
		if (activeTable == null) {
			return "Error: no active table specified!";
		}

		String constrName = parts[1];
		String type = parts[2];
		switch (type) {
			case "PRIMARY KEY": {
				if (activeTable.addPK(constrName) == 1) {
					return "Can't create Primary Key constraint!";
				}
				for (int i = 4; i < parts.length - 1; i += 3) {
					if (activeTable.addToPK(parts[i]) == 1) {
						return "Can't add attribute \"" + parts[i] + "\" to Primary Key!";
					}
				}
				break;
			}
			case "FOREIGN KEY": {
				List<ForeignKey> fkList = activeTable.getForeignKeys();
				if (fkList != null) {
					for (ForeignKey fk : fkList) {
						if (fk.getFKName().equals(parts[1])) {
							return "Error: Table " + activeTable.getTableName()
									+ " already contains Foreign Key Constraint named " + parts[1];
						}
					}
				}

				
				List<Attribute> localFields = new ArrayList<Attribute>();
				List<Attribute> referencedFields = new ArrayList<Attribute>();
				List<FKAttribute> fkAttrs = new ArrayList<FKAttribute>();
				List<IndexAttribute> indAttrs = new ArrayList<IndexAttribute>();
				int ind = 4;
				// Each local field must exist
				while (!parts[ind - 2].equals(")")) {
					System.out.println(parts[ind]);
					Attribute tmpAttr = activeTable.getAttribute(parts[ind]);
					if (tmpAttr == null) {
						return "Error: Attribute " + parts[ind] + " not found in Table " + activeTable.getTableName() + "!";
					}
					localFields.add(tmpAttr);
					ind += 3;
				}
				// referenced Table must exist
				Table referencedTable = activeDb.getTable(parts[ind]);
				if (referencedTable == null) {
					return "Error: Table " + parts[ind] + " not found!";
				}
				if (referencedTable.getPrimaryKeys() == null) {
					return "Error: Table " + referencedTable.getTableName() + " has no Primary Key!";
				}
				
				if (fkList != null) {
					for (ForeignKey fk : fkList) {
						if (fk.getReferencedTable().equals(referencedTable.getTableName())) {
							return "Error: Table " + activeTable.getTableName()
									+ " already contains Foreign Key Constraint referencing " + fk.getReferencedTable();
						}
					}
				}
				
				ind += 2;
				// Each referenced field must exist and be PK
				while (ind < parts.length) {
					System.out.println(parts[ind]);
					Attribute tmpAttr = referencedTable.getAttribute(parts[ind]);
//					referencedTable.getPrimaryKeys().get(0).get
					if (tmpAttr == null) {
						return "Error: Attribute " + parts[ind] + " not found in Table " + referencedTable.getTableName() + "!";
					}
					if (referencedFields.contains(tmpAttr)) {
						return "Error: PK Field " + referencedTable.getTableName() + "." + tmpAttr.getAttributeName() + " is referenced multiple times in one Foreign Key!";
					}
					if (!tmpAttr.isPK(referencedTable)) {
						return "Error: Foreign Key must match referenced Table's Primary Key! (" + referencedTable.getTableName() + "." + tmpAttr.getAttributeName() + " is not part of PK)!";
					}
					referencedFields.add(tmpAttr);
					ind += 3;
				}
				if (localFields.size() != referencedTable.getPrimaryKeys().size() || localFields.size() != referencedFields.size()) {
					return "Error: Foreign Key on " + activeTable.getTableName() + " must reference entire Primary Key on " + referencedTable.getTableName() + "!";
				}
				
				for (int i = 0; i < localFields.size(); i++) {
					Attribute local = localFields.get(i);
					Attribute refd = referencedFields.get(i);
					if (!local.getAttributeType().equals(refd.getAttributeType())) {
						return "Error: Type mismatch on Foreign Key (" + 
								activeTable.getTableName() + 
								"." + 
								localFields.get(i).getAttributeName() + 
								", " +
								referencedTable.getTableName() +
								"." +
								referencedFields.get(i).getAttributeName() +
								")";
					}
					fkAttrs.add(new FKAttribute(local.getAttributeName(), referencedTable.getTableName(), refd.getAttributeName()));
					indAttrs.add(new IndexAttribute(local.getAttributeName()));
				}
				
				ForeignKey activeFK = new ForeignKey(parts[1]);
				Index activeIndex = new Index(parts[1] + "_Index", "0");
				activeIndex.setIndexList(indAttrs);
//				activeIndex.addIndex(activeTable, type)
				activeFK.setFkList(fkAttrs);
//				System.out.println("FK Length: " + fkAttrs.size());
				if (activeTable.addFK(activeFK) == 1 || activeTable.addIndexFile(activeIndex) == 1) {
					return "Error: Couldn't add Foreign Key " + parts[1] + " to Table " + activeTable.getTableName();
				}
				try {
					activeMongoDb.createCollection(session, activeIndex.getIndexName());
				} catch (MongoException e) {
					return "Error: Couldn't create Foreign Key Index (Database element \"" + activeIndex.getIndexName() + "\" exists?)";
				}
				
				
				break;
			}
			case "UNIQUE": {
				if (activeTable.addUQ(constrName) == 1) {
					return "Can't create Unique Key constraint!";
				}
				for (int i = 4; i < parts.length - 1; i += 3) {

					if (activeTable.addToUQ(constrName, parts[i]) == 1) {
						return "Can't add attribute \"" + parts[i] + "\" to Unique Key!";
					}
				}
				break;
			}
			default: {
				return "Error: Unknown constraint type!";
			}
		}
		return ValidatorDictionary.ok;
	}
}
