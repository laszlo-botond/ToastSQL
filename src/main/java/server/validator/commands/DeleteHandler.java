package server.validator.commands;

import static com.mongodb.client.model.Filters.eq;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;

import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

import server.databaseElements.Attribute;
import server.databaseElements.Database;
import server.databaseElements.ForeignKey;
import server.databaseElements.Index;
import server.databaseElements.IndexAttribute;
import server.databaseElements.Table;
import server.validator.ValidatorDictionary;

public class DeleteHandler {

	public static String deleteFrom(String[] parts, ClientSession session, Database activeDb, MongoDatabase activeMongoDb, MongoCollection<Document> activeMongoTable) {
		if (activeDb == null) {
			return "Error: No active Database!";
		}
		
		Table activeTable = activeDb.getTable(parts[1]);
		if (activeTable == null) {
			return "Error: Table " + parts[1] + " not found!"; 
		}
		activeMongoTable = activeMongoDb.getCollection(parts[1]);
		
		int ind = 3;
		String[] pkArray = new String[activeTable.getPrimaryKeys().size()];
		while (ind < parts.length - 1) {
			List<Integer> posList = activeTable.getAttrPos(parts[ind]);
			if (posList.get(0) != 1) {
				return "Error: PK Attribute " + parts[ind] + " not found!";
			}
			pkArray[posList.get(1)] = parts[ind + 2].toString();
			ind += 4;
		}
		String builtPK = Arrays.stream(pkArray).collect(Collectors.joining(","));
		
		FindIterable<Document> foundDocs = activeMongoTable.find(session, eq("_id", builtPK));
		Document foundDoc = null;
		for (Document tmpDoc : foundDocs) {
			foundDoc = tmpDoc;
		}
		DeleteResult res = activeMongoTable.deleteOne(session, eq("_id", builtPK));
		if (res.getDeletedCount() == 0) {
			return ValidatorDictionary.ok;
		}
		
		// TODO: Delete from index too
		if (activeTable.getIndexFiles() != null) {
			for (Index actInd : activeTable.getIndexFiles()) {
				MongoCollection<Document> indColl = activeMongoDb.getCollection(actInd.getIndexName());
				String indPK = "";
				for (IndexAttribute indAttr : actInd.getIndexes()) {
					List<Integer> posList = activeTable.getAttrPos(indAttr.getAttr());
					
					if (posList.get(0) == 1) {
						indPK = indPK + ValidatorDictionary.partsHotfix(((String) foundDoc.get("_id")).split(","))[posList.get(1)] + ",";
					} else {
						indPK = indPK + ValidatorDictionary.partsHotfix(((String) foundDoc.get("value")).split(","))[posList.get(1)] + ",";
					}
				}
				indPK = indPK.substring(0, indPK.length() - 1);
				System.out.println(actInd.getIndexName() + ": " + indPK);
				MongoCollection<Document> actIndTable = activeMongoDb.getCollection(actInd.getIndexName());
				FindIterable<Document> foundIndDocs = actIndTable.find(session, eq("_id", indPK));
				for (Document oneIndDoc : foundIndDocs) {
					String newEntry = Arrays.stream(((String) oneIndDoc.get("value")).split("\n")).filter(st -> !st.equals(builtPK)).collect(Collectors.joining("\n"));
					actIndTable.deleteOne(session, eq("_id", indPK));
					if (!newEntry.isEmpty()) {
						Document replacedInd = new Document();
						replacedInd.put("_id", indPK);
						replacedInd.put("value", newEntry);
						actIndTable.insertOne(session, replacedInd);
					}
//					System.out.println("New index entry: " + newEntry);
					
				}
			}
		}
		
		for (Table refTable : activeTable.getReferencingTables(activeDb)) {
			for (ForeignKey fk : refTable.getForeignKeys()) {
				if (fk.getReferencedTable().equals(activeTable.getTableName())) {
					MongoCollection<Document> indCollection = activeMongoDb.getCollection(fk.getFKName() + "_Index");
					FindIterable<Document> indexDocs = indCollection.find(session, eq("_id", builtPK)); // max 1db
					for (Document refDoc : indexDocs) {
						for (String oneMatchingPK : ((String) refDoc.get("value")).split("\n")) {
							
							// TODO: ON DELETE RESTRICT (default)
							return "Error: DELETE statement violates Referential Integrity!";
					
							// TODO: ON DELETE CASCADE
//							String[] pkParts = ValidatorDictionary.partsHotfix(oneMatchingPK.split(","));
//							String newReq = "DELETE FROM ," + refTable.getTableName() + ",WHERE ,";
//							int pkInd = 0;
//							for (Attribute refAttr : refTable.getAttributes()) {
//								if (refAttr.isPK(refTable)) {
//									if (pkInd > 0) {
//										newReq = newReq + "AND ,";
//									}
//									newReq = newReq + refAttr.getAttributeName() + ",=," + pkParts[pkInd] + ",";
//									pkInd++;
//								}
//							}
//							System.out.println(newReq);
//							String newReqAns = deleteFrom(ValidatorDictionary.partsHotfix(newReq.split(",")), session, activeDb, activeMongoDb, activeMongoTable);
//							if (!newReqAns.equals(ValidatorDictionary.ok)) {
//								return newReqAns;
//							}
							
							// TODO: ON DELETE SET NULL
						}
					}
				}
			}
		}
		return ValidatorDictionary.ok;
	}
	
	
}
