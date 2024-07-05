package server.validator.commands;

import java.awt.print.Printable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.bson.Document;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import server.databaseElements.Attribute;
import server.databaseElements.Database;
import server.databaseElements.Index;
import server.databaseElements.Table;
import server.validator.ValidatorDictionary;

public class SelectHandler {

	private static final Comparator<String> customComparator = new Comparator<String>() {
		@Override
		public int compare(String s1, String s2) {
			try {
				Integer a = Integer.parseInt(s1);
				Integer b = Integer.parseInt(s2);
				return a.compareTo(b);
			} catch (NumberFormatException e) {
				try {
					Double a = Double.parseDouble(s1);
					Double b = Double.parseDouble(s2);
					return a.compareTo(b);
				} catch (NumberFormatException e2) {
					return s1.compareTo(s2);					
				}
			}
		}
	};
	
	
	public static String selectFrom(ValidatorDictionary valDict, ClientSession session, String[] parts, Database activeDb, MongoDatabase activeMongoDb) {
		List<String> selectParts = new ArrayList<String>();

		if (activeDb == null) {
			return "Error: No active Database!";
		}
		
		if (parts[0].equals("SELECT DISTINCT ")) {
			valDict.setDistinct(true);
		} else {
			valDict.setDistinct(false);
		}

		for (int i = 1; i < parts.length; i++) { // skip first "SELECT "
			if (parts[i].equals("FROM ")) {
				Table activeTable = activeDb.getTable(parts[i + 1]);
				MongoCollection<Document> activeMongoTable = activeMongoDb.getCollection(parts[i + 1]);
				
				Map<String, String> allDocuments = new TreeMap<String, String>(SelectHandler.customComparator);
				List<TreeMap<String, List<String>>> ownPKtoJointPK = new ArrayList<TreeMap<String, List<String>>>();
				TreeMap<String, List<String>> actMap = new TreeMap<String, List<String>>(SelectHandler.customComparator);
				valDict.setAllDocuments(allDocuments);
				valDict.setOwnPKtoJointPK(ownPKtoJointPK);
				
				
				valDict.setActiveTable(activeTable);
				valDict.setActiveMongoTable(activeMongoTable);
				if (activeTable == null || activeMongoTable == null) {
					return "Error: Table " + parts[i + 1] + " not found!";
				}
				
				for (Document doc : activeMongoTable.find(session)) {
					allDocuments.put(doc.get("_id").toString(), doc.get("value").toString());
					List<String> tmpList = new ArrayList<String>();
					tmpList.add(doc.get("_id").toString());
					actMap.put(doc.get("_id").toString(), tmpList);
				}
				ownPKtoJointPK.add(actMap);
				Map<String, String> tableMap = new HashMap<String, String>();
				valDict.setTableMap(tableMap);
				tableMap.put(activeTable.getTableName(), activeTable.getTableName() + ",0");
				break;
			} else {
				selectParts.add(parts[i]);
			}
		}
		
		valDict.setSelectParts(selectParts);
		return ValidatorDictionary.ok;
	}
	
	public static String executeSelect(ValidatorDictionary valDict, Table activeTable, Database activeDb, Map<String, String> allDocuments, Map<String, String> tableMap, boolean distinct) {
		
		StringBuilder ans = new StringBuilder("");
		List<List<String>> retList = parseSelectParts(valDict, tableMap, activeDb);
		List<String> fieldNames = retList.get(0);
		List<String> fieldAlias = retList.get(1);
		boolean isAggr = false;
		for (String fieldName : fieldNames) {
			if (fieldName.contains("(")) {
				isAggr = true;
			}
		}
		if (isAggr) {
			return executeNoGroupAggrSelect(valDict, activeTable, activeDb, allDocuments, tableMap, distinct);
		}
		int rowNr = 0;
		int colNr = fieldNames.size();
		
		if (!distinct) {
			// ALL entries
			for (String thisPK : allDocuments.keySet()) {
				String thisValue = allDocuments.get(thisPK);
				
				for (String fieldName : fieldNames) {
					String[] nameParts = fieldName.split("\\.");
					Table selectTable = activeTable;
					if (nameParts.length > 1) {
						selectTable = activeDb.getTable(tableMap.get(nameParts[0]).split(",")[0]);
						if (selectTable == null) {
							return "Error: Table or alias " + tableMap.get(nameParts[0].split(",")[0]) + " not found!";
						}
					}
					String thisTableName = selectTable.getTableName();
					
					String allList;
					fieldName = nameParts[nameParts.length - 1];
					List<Integer> posList = selectTable.getAttrPos(fieldName);
					if (posList.get(0) == -1) {
						return "Error: Attribute " + fieldName + " not found!";
					}
					if (posList.get(0) == 0) {
						// not PK
						allList = thisValue;
					}
					else {
						// PK
						allList = thisPK;
					}
					String[] tableData = allList.split("\n");
					
					String[] splitList = ValidatorDictionary.partsHotfix(tableData[Integer.parseInt(tableMap.get(thisTableName).split(",")[1])].split(","));
					ans.append(splitList[posList.get(1)]);
					ans.append(",");
				}
				ans.append("\n");
				rowNr++;
			}
		} else {
			// DISTINCT entries
			Set<String> answerSet = new HashSet<>();
			for (String thisPK : allDocuments.keySet()) {
				String thisValue = allDocuments.get(thisPK);
				StringBuilder thisEntry = new StringBuilder("");
				for (String fieldName : fieldNames) {
					String[] nameParts = fieldName.split("\\.");
					Table selectTable = activeTable;
					if (nameParts.length > 1) {
						selectTable = activeDb.getTable(tableMap.get(nameParts[0]).split(",")[0]);
						if (selectTable == null) {
							return "Error: Table or alias " + tableMap.get(nameParts[0].split(",")[0]) + " not found!";
						}
					}
					String thisTableName = selectTable.getTableName();
					
					String allList;
					fieldName = nameParts[nameParts.length - 1];
					List<Integer> posList = selectTable.getAttrPos(fieldName);
					if (posList.get(0) == -1) {
						return "Error: Attribute " + fieldName + " not found!";
					}
					if (posList.get(0) == 0) {
						// not PK
						allList = thisValue;
					}
					else {
						// PK
						allList = thisPK;
					}
					String[] tableData = allList.split("\n");
					
					String[] splitList = ValidatorDictionary.partsHotfix(tableData[Integer.parseInt(tableMap.get(thisTableName).split(",")[1])].split(","));
					thisEntry.append(splitList[posList.get(1)]);
					thisEntry.append(",");
				}
				if (!answerSet.contains(thisEntry.toString())) {
					answerSet.add(thisEntry.toString());
					ans.append(thisEntry);
					ans.append("\n");
					rowNr++;
				}
			}
		}
		
		
		
		String headers = fieldAlias.stream().collect(Collectors.joining(","));
//		System.out.println("ColNr: " + colNr);
//		System.out.println("Headers: " + headers);
//		System.out.println(rowNr + "\n" + colNr + "\n" + headers + "\n" + ans.toString());
		return rowNr + "\n" + colNr + "\n" + headers + "\n"+ ans.toString();
	}
	
	public static String fromAbbreviation(String[] parts, Table activeTable, Map<String, String> tableMap) {
		if (tableMap.containsKey(parts[1])) {
			return "Error: Table alias " + parts[1] + " is ambiguous!";
		}
		tableMap.put(parts[1], activeTable.getTableName() + ",0");	
		return ValidatorDictionary.ok;
	}
	
	public static String joinOn(ValidatorDictionary valDict, ClientSession session, MongoDatabase activeMongoDb, String[] parts, Database activeDb, Table fromTable, Map<String, String> allDocuments, Map<String, String> tableMap) {
		Table jointTable = activeDb.getTable(parts[1]);
		if (jointTable == null) {
			return "Error: Table " + parts[1] + " cannot be found!";
		}
		if (tableMap.containsKey(parts[1])) {
			return "Error: Table alias " + parts[1] + " is ambiguous!";
		}

		int tableOrder = tableMap.values().stream().collect(Collectors.toSet()).size();
		tableMap.put(parts[1], parts[1] + "," + tableOrder);
		int ind = 2;
		while (!parts[ind].equals("ON ")) {
			ind++;
		}
		if (ind > 2) {
			// alias existed
			String aliasName = parts[ind - 1];
			if (tableMap.containsKey(aliasName)) {
				return "Error: Table alias " + aliasName + " is ambiguous!";
			}
			tableMap.put(aliasName, parts[1] + "," + tableOrder);
		}
		
		ind++;
		// verify left-side fields of join
		String joinField1 = verifyTableField(parts, ind, tableMap, activeDb, fromTable);

		if (joinField1.split(",").length == 1) {
			return "Error: " + "Invalid join condition on table " + jointTable.getTableName();
		} 
		Table joinTable1 = activeDb.getTable(joinField1.split(",")[0]);
		Attribute joinAttr1 = joinTable1.getAttribute(joinField1.split(",")[1]);
		ind += Integer.parseInt(joinField1.split(",")[2]);
		
		ind++; // skip '='
		
		// verify right-side fields of join
		String joinField2 = verifyTableField(parts, ind, tableMap, activeDb, fromTable);
		if (joinField2.split(",").length == 1) {
			return "Error: " + "Invalid join condition on table " + jointTable.getTableName();
		} 
		Table joinTable2 = activeDb.getTable(joinField2.split(",")[0]);
		Attribute joinAttr2 = joinTable2.getAttribute(joinField2.split(",")[1]);
		ind += Integer.parseInt(joinField2.split(",")[2]);
		
		// check if one field is from joint table
		if (!joinTable1.getTableName().equals(jointTable.getTableName()) && !joinTable2.getTableName().equals(jointTable.getTableName()) ||
			joinTable1.getTableName().equals(jointTable.getTableName()) && joinTable2.getTableName().equals(jointTable.getTableName())) {
				return "Error: " + "Invalid join condition on table " + jointTable.getTableName();
		}
		
		// check if types of fields match
		if (!joinAttr1.getAttributeType().equals(joinAttr2.getAttributeType())) {
			return "Error: " + "Join condition type mismatch on table " + jointTable.getTableName();
		}
		
		joinContents(valDict, session, activeMongoDb, allDocuments, tableMap, jointTable, joinTable1, joinAttr1, joinTable2, joinAttr2);
		
		return ValidatorDictionary.ok;
	}
	
	public static void createMapJointPK(ValidatorDictionary valDict, Map<String, String> allDocuments) {
		List<TreeMap<String, List<String>>> ownPKtoJointPK = new ArrayList<>();
		boolean firstIteration = true;

		for (String jointPK : allDocuments.keySet()) {
			String rowsOfPK[] = jointPK.split("\n");
			int rowNr = 0;
			for (String oneRow : rowsOfPK) {
				if (firstIteration) {
					// initialize Map of table
					ownPKtoJointPK.add(new TreeMap<>(SelectHandler.customComparator));
				}
				Map<String, List<String>> tmpMap = ownPKtoJointPK.get(rowNr);
				if (!tmpMap.containsKey(oneRow)) {
					// initialize List of PK row
					ownPKtoJointPK.get(rowNr).put(oneRow, new ArrayList<>());
				} 
				// put into List of PK row
				ownPKtoJointPK.get(rowNr).get(oneRow).add(jointPK);
				rowNr++;
			}
			firstIteration = false;
		}
		
		valDict.setOwnPKtoJointPK(ownPKtoJointPK);
	}
	
	public static String whereCondition(ValidatorDictionary valDict, String[] parts, MongoDatabase activeMongoDb, Database activeDb, Table activeTable, Map<String, String> allDocuments, Map<String, String> tableMap) {
		
		createMapJointPK(valDict, allDocuments);
		return whereAnd(valDict, parts, activeMongoDb, activeDb, activeTable, allDocuments, valDict.getPKmap(), tableMap);
	}
	
	public static String whereAnd(ValidatorDictionary valDict, String[] parts, MongoDatabase activeMongoDb, Database activeDb, Table activeTable, Map<String, String> allDocuments, List<TreeMap<String, List<String>>> ownPKtoJointPK, Map<String, String> tableMap) {
		
		boolean fixedValue1 = false;
		String value1 = "";
		Table whereTable1 = null;
		Attribute whereAttr1 = null;
		List<Integer> wherePosList1 = null;
		int tableNr1 = -1;
		int operatorInd = 2;
		
		String[] resp1 = parseWherePart(parts, 1, activeDb, activeTable, tableMap).split("\n");
		// check error
		if (resp1.length == 1) {
			return resp1[0];
		}

		fixedValue1 = Boolean.parseBoolean(resp1[0]);
		if (fixedValue1) {
			value1 = resp1[1];
		} else {
			whereTable1 = activeDb.getTable(resp1[2]);
			whereAttr1 = whereTable1.getAttribute(resp1[3]);
			wherePosList1 = new ArrayList<>();
			wherePosList1.add(Integer.parseInt(resp1[4]));
			wherePosList1.add(Integer.parseInt(resp1[5]));
			tableNr1 = Integer.parseInt(resp1[6]);
		}
		operatorInd = Integer.parseInt(resp1[7]);
		
		String operator = parts[operatorInd];
		
		boolean fixedValue2 = false;
		String value2 = "";
		Table whereTable2 = null;
		Attribute whereAttr2 = null;
		List<Integer> wherePosList2 = null;
		int tableNr2 = -1;
		
		String[] resp2 = parseWherePart(parts, operatorInd+1, activeDb, activeTable, tableMap).split("\n");
		if (resp2.length == 1) {
			return resp2[0];
		}
		
		fixedValue2 = Boolean.parseBoolean(resp2[0]);
		if (fixedValue2) {
			value2 = resp2[1];
		} else {
			whereTable2 = activeDb.getTable(resp2[2]);
			whereAttr2 = whereTable2.getAttribute(resp2[3]);
			wherePosList2 = new ArrayList<>();
			wherePosList2.add(Integer.parseInt(resp2[4]));
			wherePosList2.add(Integer.parseInt(resp2[5]));
			tableNr2 = Integer.parseInt(resp2[6]);
		}
		
		
		List<String> keysToRemove = new ArrayList<>();
		if (fixedValue1 && fixedValue2) {
			return "Error: Comparing two constants in WHERE statement! (" + value1 + ", " + value2 + ")";
		}
		
		if (!fixedValue1 && !fixedValue2) {
			String type = whereAttr1.getAttributeType().toLowerCase();
			if (!whereAttr1.getAttributeType().equals(whereAttr2.getAttributeType())) {
				return "Error: Type mismatch in WHERE statement! (" + whereAttr1.getAttributeName() + ", " + whereAttr2.getAttributeName() + ")";
			}
			for (String jointPK : allDocuments.keySet()) {
				String compareValue1 = "";
				if (wherePosList1.get(0) == 1) {
					compareValue1 = jointPK.split("\n")[tableNr1].split(",")[wherePosList1.get(1)];
				}
				else {
					compareValue1 = allDocuments.get(jointPK).split("\n")[tableNr1].split(",")[wherePosList1.get(1)];
				}
				
				String compareValue2 = "";
				if (wherePosList2.get(0) == 1) {
					compareValue2 = jointPK.split("\n")[tableNr2].split(",")[wherePosList2.get(1)];
				}
				else {
					compareValue2 = allDocuments.get(jointPK).split("\n")[tableNr2].split(",")[wherePosList2.get(1)];
				}
				
				if (compareValue1.equals("null") || compareValue2.equals("null")) {
					keysToRemove.add(jointPK);
					continue;
				}
				
				
				if (!applyOperator(compareValue1, operator, compareValue2, type)) {
					keysToRemove.add(jointPK);
					// allDocuments.remove(jointPK);
					int indexRowNr = 0;
					for (String onePK : jointPK.split("\n")) {
						ownPKtoJointPK.get(indexRowNr).get(onePK).remove(jointPK);
						indexRowNr++;
					}
				}
			}
		} else {
			String whereValue;
			Table whereTable;
			Attribute whereAttr;
			List<Integer> wherePosList;
			int tableNr;
			if (fixedValue1) {
				whereValue = value1;
				whereTable = whereTable2;
				whereAttr = whereAttr2;
				wherePosList = wherePosList2;
				tableNr = tableNr2;
			} else {
				whereValue = value2;
				whereTable = whereTable1;
				whereAttr = whereAttr1;
				wherePosList = wherePosList1;
				tableNr = tableNr1;
			}
			
			
			String type = whereAttr.getAttributeType().toLowerCase();
			if (ValidatorDictionary.parseAttribute(new Document(), whereAttr, whereValue) == 1) {
				return "Error: Type mismatch in WHERE statement: Value: " + whereValue;
			}
			
			
			Index whereIndex = whereTable.getIndexOnAttribute(whereAttr);
			// if attribute is PK
			if (whereTable.getPrimaryKeys() != null && whereTable.getPrimaryKeys().size() == 1 && whereTable.getPrimaryKeys().get(0).getPKAttributeName().equals(whereAttr.getAttributeName())) {
//				System.out.println("Where PK");
				TreeMap<String, List<String>> wherePKtoJointPK = ownPKtoJointPK.get(tableNr);
				List<String> matchingJointPKs = new ArrayList<>();
				switch(operator) {
					case "=": {
						if (wherePKtoJointPK.containsKey(whereValue)) {
							matchingJointPKs = wherePKtoJointPK.get(whereValue);
						}
						break;
					}
					case "<>": {
						String nextKey = wherePKtoJointPK.higherKey(whereValue);
						while (nextKey != null) {
							for (String pk : wherePKtoJointPK.get(nextKey)) {
								matchingJointPKs.add(pk);
							}
							nextKey = wherePKtoJointPK.higherKey(nextKey);
						}
						
						nextKey = wherePKtoJointPK.lowerKey(whereValue);
						while (nextKey != null) {
							for (String pk : wherePKtoJointPK.get(nextKey)) {
								matchingJointPKs.add(pk);
							}
							nextKey = wherePKtoJointPK.lowerKey(nextKey);
						}
						break;
					}
					case "<": {
						if (fixedValue2) {
							String nextKey = wherePKtoJointPK.lowerKey(whereValue);
							while (nextKey != null) {
								for (String pk : wherePKtoJointPK.get(nextKey)) {
									matchingJointPKs.add(pk);
								}
								nextKey = wherePKtoJointPK.lowerKey(nextKey);
							}
						}
						else {
							String nextKey = wherePKtoJointPK.higherKey(whereValue);
							while (nextKey != null) {
								for (String pk : wherePKtoJointPK.get(nextKey)) {
									matchingJointPKs.add(pk);
								}
								nextKey = wherePKtoJointPK.higherKey(nextKey);
							}	
						}
						break;
					}
					case "<=": {
						if (wherePKtoJointPK.containsKey(whereValue)) {
							matchingJointPKs = wherePKtoJointPK.get(whereValue);
						}
						if (fixedValue2) {
							String nextKey = wherePKtoJointPK.lowerKey(whereValue);
							while (nextKey != null) {
								for (String pk : wherePKtoJointPK.get(nextKey)) {
									matchingJointPKs.add(pk);
								}
								nextKey = wherePKtoJointPK.lowerKey(nextKey);
							}
						}
						else {
							String nextKey = wherePKtoJointPK.higherKey(whereValue);
							while (nextKey != null) {
								for (String pk : wherePKtoJointPK.get(nextKey)) {
									matchingJointPKs.add(pk);
								}
								nextKey = wherePKtoJointPK.higherKey(nextKey);
							}	
						}
						break;
					}
					case ">": {
						if (fixedValue2) {
							String nextKey = wherePKtoJointPK.higherKey(whereValue);
							while (nextKey != null) {
								for (String pk : wherePKtoJointPK.get(nextKey)) {
									matchingJointPKs.add(pk);
								}
								nextKey = wherePKtoJointPK.higherKey(nextKey);
							}
						}
						else {
							String nextKey = wherePKtoJointPK.lowerKey(whereValue);
							while (nextKey != null) {
								for (String pk : wherePKtoJointPK.get(nextKey)) {
									matchingJointPKs.add(pk);
								}
								nextKey = wherePKtoJointPK.lowerKey(nextKey);
							}
						}
						break;
					}
					case ">=": {
						if (wherePKtoJointPK.containsKey(whereValue)) {
							matchingJointPKs = wherePKtoJointPK.get(whereValue);
						}
						if (fixedValue2) {
							String nextKey = wherePKtoJointPK.higherKey(whereValue);
							while (nextKey != null) {
								for (String pk : wherePKtoJointPK.get(nextKey)) {
									matchingJointPKs.add(pk);
								}
								nextKey = wherePKtoJointPK.higherKey(nextKey);
							}
						}
						else {
							String nextKey = wherePKtoJointPK.lowerKey(whereValue);
							while (nextKey != null) {
								for (String pk : wherePKtoJointPK.get(nextKey)) {
									matchingJointPKs.add(pk);
								}
								nextKey = wherePKtoJointPK.lowerKey(nextKey);
							}	
						}
						break;
					}
					default: {
						
					}
				}
				Map<String, String> newDocuments = new HashMap<>();
				if (matchingJointPKs != null) {
					for (String matchingJointPK : matchingJointPKs) {
						
						if (allDocuments.containsKey(matchingJointPK)) {
							newDocuments.put(matchingJointPK, allDocuments.get(matchingJointPK));
						}
						
					}
				}
				valDict.setAllDocuments(newDocuments);
			}
			
			// if attribute has Index
			else if (whereIndex != null) {
				// System.out.println("Where INDEX");
				TreeMap<String, String> indexMap = new TreeMap<>(SelectHandler.customComparator);
				for (Document doc : activeMongoDb.getCollection(whereIndex.getIndexName()).find()) {
					indexMap.put(doc.get("_id").toString(), doc.get("value").toString());
				}
				
				
				TreeMap<String, List<String>> wherePKtoJointPK = ownPKtoJointPK.get(tableNr);
				List<String> matchingJointPKs = new ArrayList<>();
				
				switch(operator) {
					case "=": {
						if (indexMap.containsKey(whereValue)) {
							String wherePKs = indexMap.get(whereValue);
							for (String onePK : wherePKs.split("\n")) {
								for (String oneJointPK : wherePKtoJointPK.get(onePK)) {
									matchingJointPKs.add(oneJointPK);
								}
							}
						}
						break;
					}
					case "<>": {
						for (String indexKey : indexMap.keySet()) {
							if (!indexKey.equals(whereValue)) {
								String wherePKs = indexMap.get(indexKey);
								for (String onePK : wherePKs.split("\n")) {
									for (String oneJointPK : wherePKtoJointPK.get(onePK)) {
										matchingJointPKs.add(oneJointPK);
									}
								}
							}
						}
						break;
					}
					case "<": {
						if (fixedValue2) {
							String nextKey = indexMap.lowerKey(whereValue);
							while (nextKey != null) {
								String wherePKs = indexMap.get(nextKey);
								for (String onePK : wherePKs.split("\n")) {
									for (String oneJointPK : wherePKtoJointPK.get(onePK)) {
										matchingJointPKs.add(oneJointPK);
									}
								}
								nextKey = indexMap.lowerKey(nextKey);
							}
						}
						else {
							String nextKey = indexMap.higherKey(whereValue);
							while (nextKey != null) {
								String wherePKs = indexMap.get(nextKey);
								for (String onePK : wherePKs.split("\n")) {
									for (String oneJointPK : wherePKtoJointPK.get(onePK)) {
										matchingJointPKs.add(oneJointPK);
									}
								}
								nextKey = indexMap.higherKey(nextKey);
							}
						}
						break;
					}
					case "<=": {
						if (fixedValue2) {
							String nextKey = indexMap.lowerKey(whereValue);
							if (indexMap.containsKey(whereValue)) {
								nextKey = whereValue;
							}
							
							while (nextKey != null) {
								String wherePKs = indexMap.get(nextKey);
								for (String onePK : wherePKs.split("\n")) {
									for (String oneJointPK : wherePKtoJointPK.get(onePK)) {
										matchingJointPKs.add(oneJointPK);
									}
								}
								nextKey = indexMap.lowerKey(nextKey);
							}
						}
						else {
							String nextKey = indexMap.higherKey(whereValue);
							if (indexMap.containsKey(whereValue)) {
								nextKey = whereValue;
							}
							
							while (nextKey != null) {
								String wherePKs = indexMap.get(nextKey);
								for (String onePK : wherePKs.split("\n")) {
									for (String oneJointPK : wherePKtoJointPK.get(onePK)) {
										matchingJointPKs.add(oneJointPK);
									}
								}
								nextKey = indexMap.higherKey(nextKey);
							}	
						}
						break;
					}
					case ">": {
						if (fixedValue2) {
							String nextKey = indexMap.higherKey(whereValue);
							while (nextKey != null) {
								String wherePKs = indexMap.get(nextKey);
								for (String onePK : wherePKs.split("\n")) {
									for (String oneJointPK : wherePKtoJointPK.get(onePK)) {
										matchingJointPKs.add(oneJointPK);
									}
								}
								nextKey = indexMap.higherKey(nextKey);
							}
						}
						else {
							String nextKey = indexMap.lowerKey(whereValue);
							while (nextKey != null) {
								String wherePKs = indexMap.get(nextKey);
								for (String onePK : wherePKs.split("\n")) {
									for (String oneJointPK : wherePKtoJointPK.get(onePK)) {
										matchingJointPKs.add(oneJointPK);
									}
								}
								nextKey = indexMap.lowerKey(nextKey);
							}	
						}
						break;
					}
					case ">=": {
						if (fixedValue2) {
							String nextKey = indexMap.higherKey(whereValue);
							if (indexMap.containsKey(whereValue)) {
								nextKey = whereValue;
							}
							
							while (nextKey != null) {
								String wherePKs = indexMap.get(nextKey);
								for (String onePK : wherePKs.split("\n")) {
									for (String oneJointPK : wherePKtoJointPK.get(onePK)) {
										matchingJointPKs.add(oneJointPK);
									}
								}
								nextKey = indexMap.higherKey(nextKey);
							}
						}
						else {
							String nextKey = indexMap.lowerKey(whereValue);
							if (indexMap.containsKey(whereValue)) {
								nextKey = whereValue;
							}
							while (nextKey != null) {
								String wherePKs = indexMap.get(nextKey);
								for (String onePK : wherePKs.split("\n")) {
									for (String oneJointPK : wherePKtoJointPK.get(onePK)) {
										matchingJointPKs.add(oneJointPK);
									}
								}
								nextKey = indexMap.lowerKey(nextKey);
							}	
						}
						break;
					}
					default: {
						
					}
				}
				Map<String, String> newDocuments = new HashMap<>();
				if (matchingJointPKs != null) {
					for (String oneMatchingJointPK : matchingJointPKs) {
						if (allDocuments.containsKey(oneMatchingJointPK)) {
							newDocuments.put(oneMatchingJointPK, allDocuments.get(oneMatchingJointPK));
						}
					}
				}
				valDict.setAllDocuments(newDocuments);
			}
			// any other attribute
			else {
				String tableValue = "";
				for (String onePK : allDocuments.keySet()) {
					if (wherePosList.get(0) == 1) {
						// part of PK
						tableValue = ValidatorDictionary.partsHotfix(onePK.split("\n")[tableNr].split(","))[wherePosList.get(1)];
					} else {
						String oneValue = allDocuments.get(onePK);
						tableValue = ValidatorDictionary.partsHotfix(oneValue.split("\n")[tableNr].split(","))[wherePosList.get(1)];
					}
					
					if (tableValue.equals("null")) {
						keysToRemove.add(onePK);
						continue;
					}
					
					if ((fixedValue2 && !applyOperator(tableValue, operator, whereValue, type)) || (fixedValue1 && !applyOperator(whereValue, operator, tableValue, type))) {
						keysToRemove.add(onePK);
					}
				}
			}
			
			
		}
		
		for (String removeKey : keysToRemove) {
			allDocuments.remove(removeKey);
		}
		
		return ValidatorDictionary.ok;
	}
	
	private static boolean applyOperator(String value1, String operator, String value2, String type) {
		if (type.equals("int")) {
			Integer tmp1 = Integer.parseInt(value1);
			Integer tmp2 = Integer.parseInt(value2);
			return executeOperation(tmp1, tmp2, operator);
		} else if (type.equals("float")) {
			Float tmp1 = Float.parseFloat(value1);
			Float tmp2 = Float.parseFloat(value2);
			return executeOperation(tmp1, tmp2, operator);
		} else if (type.equals("double")) {
			Double tmp1 = Double.parseDouble(value1);
			Double tmp2 = Double.parseDouble(value2);
			return executeOperation(tmp1, tmp2, operator);
		}
		
		return executeOperation(value1, value2, operator);
		
	}
	
	public static boolean executeOperation(Comparable t1, Comparable t2, String operator) {
		switch (operator) {
			case "=": {
				return t1.compareTo(t2) == 0;
			}
			case "<": {
				return t1.compareTo(t2) < 0;
			}
			case ">": {
				return t1.compareTo(t2) > 0;
			}
			case ">=": {
				return t1.compareTo(t2) >= 0;
			}
			case "<=": {
				return t1.compareTo(t2) <= 0;
			}
			case "<>": {
				return t1.compareTo(t2) != 0;
			}
			default: {
				return false;
			}
		}
	}
	
	private static String parseWherePart(String[] parts, int startIndex, Database activeDb, Table activeTable, Map<String, String> tableMap) {
		boolean fixedValue1 = false;
		String value1 = "?";
		Table whereTable1 = null;
		Attribute whereAttr1 = null;
		List<Integer> wherePosList1 = null;
		int tableNr1 = -1;
		int operatorInd = startIndex + 1;
		
		if (parts.length > startIndex + 1 && parts[startIndex + 1].equals(".")) {
			if (parts[startIndex].matches("^\\d+")) {
				fixedValue1 = true;
				value1 = parts[startIndex] + "." + parts[startIndex + 2];
			} else {
				if (!tableMap.containsKey(parts[startIndex]) || activeDb.getTable(tableMap.get(parts[startIndex]).split(",")[0]) == null) {
					return "Error: Table or alias " + parts[startIndex] + " not found!";
				}
				whereTable1 = activeDb.getTable(tableMap.get(parts[startIndex]).split(",")[0]);
				whereAttr1 = whereTable1.getAttribute(parts[startIndex + 2]);
				if (whereAttr1 == null) {
					return "Error: Attribute " + parts[startIndex + 2] + " not found in table " + whereTable1.getTableName();
				}
				tableNr1 = Integer.parseInt(tableMap.get(parts[startIndex]).split(",")[1]);
				wherePosList1 = whereTable1.getAttrPos(whereAttr1.getAttributeName());
			}
			operatorInd = startIndex + 3;
		}
		else {
			Attribute tmpAttr = activeTable.getAttribute(parts[startIndex]);
			if (tmpAttr != null) {
				whereTable1 = activeTable;
				whereAttr1 = tmpAttr;
				wherePosList1 = whereTable1.getAttrPos(whereAttr1.getAttributeName());
				tableNr1 = 0;
			} else {
				fixedValue1 = true;
				value1 = parts[startIndex];
			}
		}
		
		String tableStr = "?";
		String attrStr = "?";
		String posList1 = "?";
		String posList2 = "?";
		if (whereTable1 != null) {
			tableStr = whereTable1.getTableName();
		}
		if (whereAttr1 != null) {
			attrStr = whereAttr1.getAttributeName();
		}
		if (wherePosList1 != null) {
			posList1 = wherePosList1.get(0).toString();
			posList2 = wherePosList1.get(1).toString();
		}
		
//		System.out.println("" + fixedValue1 + "---" + value1 + "---" + tableStr + "---" + attrStr + "---" + posList1 + "---" + posList2 + "---" + tableNr1 + "---" + operatorInd);
		return "" + fixedValue1 + "\n" + value1 + "\n" + tableStr + "\n" + attrStr + "\n" + posList1 + "\n" + posList2 + "\n" + tableNr1 + "\n" + operatorInd;
	}
	
	
	private static void joinContents(ValidatorDictionary valDict,
			ClientSession session,
			MongoDatabase activeMongoDb,
			Map<String, String> allDocuments,
			Map<String, String> tableMap,
			Table jointTable,
			Table joinTable1,
			Attribute joinAttr1,
			Table joinTable2,
			Attribute joinAttr2) {
		
		Map<String, String> newDocuments = new HashMap<String, String>();
		
		Table innerTable = joinTable2;
		Attribute innerAttr = joinAttr2;
		Table outerTable = joinTable1;
		Attribute outerAttr = joinAttr1;
		if (joinTable1 == jointTable) {
			innerTable = joinTable1;
			innerAttr = joinAttr1;
			outerTable = joinTable2;
			outerAttr = joinAttr2;
		}
		
		List<Integer> innerPosList = innerTable.getAttrPos(innerAttr.getAttributeName());
		List<Integer> outerPosList = outerTable.getAttrPos(outerAttr.getAttributeName());
		int outerTableNr = Integer.parseInt(tableMap.get(outerTable.getTableName()).split(",")[1]);
		MongoCollection<Document> innerCollection = activeMongoDb.getCollection(innerTable.getTableName());
		Index innerIndex = innerTable.getIndexOnAttribute(innerAttr);
		
		if (innerTable.getPrimaryKeys() != null && innerTable.getPrimaryKeys().size() == 1 && innerTable.getPrimaryKeys().get(0).getPKAttributeName().equals(innerAttr.getAttributeName())) {
			Map<String, String> innerTableEntries = new HashMap<>();
			for (Document doc : innerCollection.find(session)) {
				innerTableEntries.put(doc.get("_id").toString(), doc.get("value").toString());
			}
			
			for (String oldPK : allDocuments.keySet()) {
				String oldValue = allDocuments.get(oldPK);
				String outerAttrValue;
				if (outerPosList.get(0) == 0) {
					// not PK
					outerAttrValue = ValidatorDictionary.partsHotfix(oldValue.split("\n")[outerTableNr].split(","))[outerPosList.get(1)];
				} else {
					// PK
					outerAttrValue = ValidatorDictionary.partsHotfix(oldPK.split("\n")[outerTableNr].split(","))[outerPosList.get(1)];
				}
				Object foundRecord = innerTableEntries.get(outerAttrValue);
				if (foundRecord != null) {
					String newPK = oldPK + "\n" + outerAttrValue;
					String newValue = oldValue + "\n" + foundRecord.toString();
					
					newDocuments.put(newPK, newValue);
				}

			}
		}
		else if (innerIndex != null) {
			// System.out.println("Index JOIN");
			MongoCollection<Document> indexCollection = activeMongoDb.getCollection(innerIndex.getIndexName());
			Map<String, String> allIndexEntries = new HashMap<>();
			Map<String, String> innerTableEntries = new HashMap<>();
			for (Document doc : indexCollection.find(session)) {
				allIndexEntries.put(doc.get("_id").toString(), doc.get("value").toString());
			}
			for (Document doc : innerCollection.find(session)) {
				innerTableEntries.put(doc.get("_id").toString(), doc.get("value").toString());
			}

			for (String oldPK : allDocuments.keySet()) {
				String oldValue = allDocuments.get(oldPK);
				String outerAttrValue;

				if (outerPosList.get(0) == 0) {
					// not PK
					outerAttrValue = ValidatorDictionary.partsHotfix(oldValue.split("\n")[outerTableNr].split(","))[outerPosList.get(1)];
				} else {
					// PK
					outerAttrValue = ValidatorDictionary.partsHotfix(oldPK.split("\n")[outerTableNr].split(","))[outerPosList.get(1)];
				}

				Object foundRecord = allIndexEntries.get(outerAttrValue);
				if (foundRecord != null) {
					String[] matchingPKs = foundRecord.toString().split("\n");

					for (String onePK : matchingPKs) {
						
						String newPK = oldPK + "\n" + onePK;
						String newValue = allDocuments.get(oldPK) + "\n" + innerTableEntries.get(onePK);
						newDocuments.put(newPK, newValue);
						
					}
				}
			}
		} else {
			Map<String, String> innerTableEntries = new HashMap<>();
			for (Document doc : innerCollection.find(session)) {
				innerTableEntries.put(doc.get("_id").toString(), doc.get("value").toString());
			}
//			int itr = 0;
			for (String oldPK : allDocuments.keySet()) {
//				System.out.println(++itr + ": " + oldPK);
				String oldValue = allDocuments.get(oldPK);
				String outerAttrValue;
				String innerAttrValue;
				if (outerPosList.get(0) == 0) {
					// not PK
					outerAttrValue = ValidatorDictionary.partsHotfix(oldValue.split("\n")[outerTableNr].split(","))[outerPosList.get(1)];
				} else {
					// PK
					outerAttrValue = ValidatorDictionary.partsHotfix(oldPK.split("\n")[outerTableNr].split(","))[outerPosList.get(1)];
				}
				for (String innerPK : innerTableEntries.keySet()) {
					String innerValue = innerTableEntries.get(innerPK);
					if (innerPosList.get(0) == 0) {
						innerAttrValue = ValidatorDictionary.partsHotfix(innerValue.split(","))[innerPosList.get(1)];
					} else {
						innerAttrValue = ValidatorDictionary.partsHotfix(innerPK.split(","))[innerPosList.get(1)];
					}
					
					if (outerAttrValue.equals(innerAttrValue)) {
						String newPK = oldPK + "\n" + innerPK;
						String newValue = allDocuments.get(oldPK) + "\n" + innerValue;
						newDocuments.put(newPK, newValue);
					}
					
				}
			}
		}
		valDict.setAllDocuments(newDocuments);
	}
	
	public static String groupBy(ValidatorDictionary valDict, String[] parts, Map<String, String> tableMap, Database activeDb, Table fromTable, Map<String, String> allDocuments, boolean distinct) {
		int ind = 1;
		List<Table> groupTables = new ArrayList<>();
		List<Attribute> groupAttributes = new ArrayList<>();
		while (ind < parts.length) {
//			System.out.println("Active part: " + parts[ind] + "," + parts[ind+1]);
			String fieldInfo = verifyTableField(parts, ind, tableMap, activeDb, fromTable);
//			System.out.println("Field Info: " + fieldInfo);
			if (fieldInfo.split(",").length == 1) {
				return "Error: GROUP BY field not found!";
			} 
			Table groupTable = activeDb.getTable(fieldInfo.split(",")[0]);
			Attribute groupAttr = groupTable.getAttribute(fieldInfo.split(",")[1]);
			ind += Integer.parseInt(fieldInfo.split(",")[2]);
			
			groupTables.add(groupTable);
			groupAttributes.add(groupAttr);
			
			ind += 2; // skip comma
		}
		
		return executeGroupSelect(valDict, fromTable, activeDb, allDocuments, tableMap, distinct, groupTables, groupAttributes);
	}
	
	private static String executeGroupSelect(ValidatorDictionary valDict, Table activeTable, Database activeDb, Map<String, String> allDocuments, Map<String, String> tableMap, boolean distinct, List<Table> groupTables, List<Attribute> groupAttributes) {
		StringBuilder ans = new StringBuilder("");
		List<List<String>> retList = parseSelectParts(valDict, tableMap, activeDb);
		List<String> fieldNames = retList.get(0);
		List<String> fieldAlias = retList.get(1);
		List<List<Integer>> groupPosLists = new ArrayList<>(); //selectTable.getAttrPos(fieldName);
		Map<String, Map<String, String>> groupResults = new HashMap<>();
		Set<String> distinctAnswerSet = new HashSet<>();
		int groupFieldsSize = groupAttributes.size();
		
		// check if ungrouped attribute appears in select list
		for (String thisFieldName : fieldNames) {
			if (thisFieldName.contains("(")) {
				continue;
			}
			
			boolean ok = false;
			String completeFieldName = thisFieldName;
			if (thisFieldName.split("\\.").length == 1) {
				completeFieldName = activeTable.getTableName() + "." + thisFieldName;
			} else {
				completeFieldName = tableMap.get(thisFieldName.split("\\.")[0]).split(",")[0] + "." + thisFieldName.split("\\.")[1];
			}
			
			for (int i=0; i<groupFieldsSize; i++) {
				if (completeFieldName.equals(groupTables.get(i).getTableName() + "." + groupAttributes.get(i).getAttributeName())) {
					ok = true;
					break;
				}
			}
			if (!ok) {
				return "Error: Attribute " + thisFieldName + " in SELECT list is not in GROUP BY clause!";
			}
		}
		
		for (int i=0; i<groupFieldsSize; i++) {
			List<Integer> thisGroupPosList = groupTables.get(i).getAttrPos(groupAttributes.get(i).getAttributeName());
			groupPosLists.add(thisGroupPosList);
		}
		
		// categorize entries into groups
		for (String thisPK : allDocuments.keySet()) {
			String thisValue = allDocuments.get(thisPK);
			StringBuilder thisGroupKey = new StringBuilder("");
			String allList = "";
			
			for (int i=0; i<groupFieldsSize; i++) {
				List<Integer> thisGroupPosList = groupPosLists.get(i);
			
				if (thisGroupPosList.get(0) == 0) {
					// not PK
					allList = thisValue;
				}
				else {
					// PK
					allList = thisPK;
				}
				String[] tableData = allList.split("\n");
				
				String[] splitList = ValidatorDictionary.partsHotfix(tableData[Integer.parseInt(tableMap.get(groupTables.get(i).getTableName()).split(",")[1])].split(","));
				thisGroupKey.append(splitList[thisGroupPosList.get(1)]);
				thisGroupKey.append(",");
			}
			
			String groupKey = thisGroupKey.toString();
			if (!groupResults.keySet().contains(groupKey)) {
				groupResults.put(groupKey, new HashMap<String, String>()); // create new map
			}			
			groupResults.get(groupKey).put(thisPK, thisValue); // add to map
		}
		
		int rowNr = 0;
		int colNr = fieldNames.size();
		
		List<String> aggregateFields = new ArrayList<>();
		List<String> aggregateTypes = new ArrayList<>();
		for (String thisFieldName : fieldNames) {
			if (thisFieldName.contains("(")) {
				String aggregateField = thisFieldName.split("\\(")[1].split("\\)")[0];
				String aggregateType = thisFieldName.split("\\(")[0].toUpperCase();
				String aTable;
				String aField;
				if (aggregateField.contains(".")) {
					aTable = tableMap.get(aggregateField.split("\\.")[0]).split(",")[0];
					aField = aggregateField.split("\\.")[1];	
				} else {
					aTable = activeTable.getTableName();
					aField = aggregateField;
				}
				
				if (aTable == null || activeDb.getTable(aTable) == null || activeDb.getTable(aTable).getAttribute(aField) == null) {
					return "Error: Attribute " + aggregateField + " does not exist!";
				}
				
				boolean numericFunction = false;
				boolean numericData = false;
				if (aggregateType.equals("AVG") || aggregateType.equals("SUM") || aggregateType.equals("MIN") || aggregateType.equals("MAX")) {
					numericFunction = true;
				}
				String dataType = activeDb.getTable(aTable).getAttribute(aField).getAttributeType().toLowerCase();
				if (dataType.equals("float") || dataType.equals("double") || dataType.equals("int")) {
					numericData = true;
				}
				if (numericFunction && !numericData) {
					return "Error: " + aggregateType + "() is not defined for type " + dataType + "!";
				}
				
				aggregateFields.add(aTable + "." + aField);
				aggregateTypes.add(aggregateType);
			}
		}
		
		int aggrSize = aggregateFields.size();
		List<String> aggrValues = new ArrayList<>();
		
		
		for (String thisGroupKey : groupResults.keySet()) {
			aggrValues = new ArrayList<>();
			for (int i=0; i<aggrSize; i++) {
				aggrValues.add("");
			}
			Map<String,String> entries = groupResults.get(thisGroupKey);
			for (String thisPK : entries.keySet()) {
				String thisValue = entries.get(thisPK);
				
				for (int i=0; i<aggrSize; i++) {
					String tableName = aggregateFields.get(i).split("\\.")[0];
					String attrName = aggregateFields.get(i).split("\\.")[1];
					List<Integer> posList = activeDb.getTable(tableName).getAttrPos(attrName);
					String allList;
					if (posList.get(0) == 0) {
						allList = thisValue;
					} else {
						allList = thisPK;
					}
					String[] tableData = allList.split("\n");
					
					String[] splitList = ValidatorDictionary.partsHotfix(tableData[Integer.parseInt(tableMap.get(tableName).split(",")[1])].split(","));
					String value = splitList[posList.get(1)];
					if (value.equals("null")) {
						continue;
					}
					String type = activeDb.getTable(tableName).getAttribute(attrName).getAttributeType().toLowerCase();
					boolean numericType = false;
					if (type.equals("float") || type.equals("int") || type.equals("double")) {
						numericType = true;
					}
					
					switch (aggregateTypes.get(i).toUpperCase()) {
						case "SUM": {
							Double partialSum = 0.0;
							if (aggrValues.get(i).isEmpty()) {
								partialSum = 0.0;
							} else {
								partialSum = Double.parseDouble(aggrValues.get(i));
							}
							partialSum = partialSum + Double.parseDouble(value);
							aggrValues.set(i, partialSum.toString());
							break;
						}
						case "AVG": {
							Double partialSum = 0.0;
							Integer rowCount = 0;
							if (aggrValues.get(i).isEmpty()) {
								partialSum = 0.0;
								rowCount = 0;
							} else {
								partialSum = Double.parseDouble(aggrValues.get(i).split(",")[0]);
								rowCount = Integer.parseInt(aggrValues.get(i).split(",")[1]);
							}
							partialSum = partialSum + Double.parseDouble(value);
							rowCount = rowCount + 1;
							aggrValues.set(i, partialSum.toString() + "," + rowCount.toString());
							break;
						}
						case "COUNT": {
							Integer rowCount = 0;
							if (aggrValues.get(i).isEmpty()) {
								rowCount = 0;
							} else {
								rowCount = Integer.parseInt(aggrValues.get(i));
							}
							rowCount = rowCount + 1;
							aggrValues.set(i, rowCount.toString());
							break;
						}
						case "MIN": {
							if (numericType) {
								Double currentValue = Double.parseDouble(value);
								if (aggrValues.get(i).isEmpty()) {
									aggrValues.set(i, currentValue.toString());
								} else {
									Double oldValue = Double.parseDouble(aggrValues.get(i));
									Double minDouble = Math.min(oldValue, currentValue);
									aggrValues.set(i, minDouble.toString());
								}
							} else {
								if (aggrValues.get(i).isEmpty()) {
									aggrValues.set(i, value);
								} else {
									String oldValue = aggrValues.get(i);
									if (value.compareTo(oldValue) == -1) {
										aggrValues.set(i, value);
									}
								}
							}
							break;
						}
						case "MAX": {
							if (numericType) {
								Double currentValue = Double.parseDouble(value);
								if (aggrValues.get(i).isEmpty()) {
									aggrValues.set(i, currentValue.toString());
								} else {
									Double oldValue = Double.parseDouble(aggrValues.get(i));
									Double minDouble = Math.max(oldValue, currentValue);
									aggrValues.set(i, minDouble.toString());
								}
							} else {
								if (aggrValues.get(i).isEmpty()) {
									aggrValues.set(i, value);
								} else {
									String oldValue = aggrValues.get(i);
									if (value.compareTo(oldValue) == 1) {
										aggrValues.set(i, value);
									}
								}
							}
							break;
						}
					}
				}
				
			} // end of thisPK
			
			for (int i=0; i<aggrSize; i++) {
				if (aggrValues.get(i).isEmpty()) {
					if (aggregateTypes.get(i).toUpperCase().equals("COUNT")) {
						aggrValues.set(i, "0");
					} else {
						aggrValues.set(i, "null");
					}
				} else if (aggregateTypes.get(i).toUpperCase().equals("AVG")) {
					Double sum = Double.parseDouble(aggrValues.get(i).split(",")[0]);
					Double count = Double.parseDouble(aggrValues.get(i).split(",")[1]);
					Double avg = sum/count;
					aggrValues.set(i, avg.toString());
				}
			}
			
			StringBuilder thisAns = new StringBuilder("");
			int aggrIterInd = 0;
			for (String thisFieldName : fieldNames) {
				if (thisFieldName.contains("(")) {
					thisAns.append(aggrValues.get(aggrIterInd));
					thisAns.append(",");
					aggrIterInd += 1;
				} else {
					String completeFieldName = thisFieldName;
					if (thisFieldName.split("\\.").length == 1) {
						completeFieldName = activeTable.getTableName() + "." + thisFieldName;
					} else {
						completeFieldName = tableMap.get(thisFieldName.split("\\.")[0]).split(",")[0] + "." + thisFieldName.split("\\.")[1];
					}
					String onePK = entries.keySet().iterator().next();
					String tableName = completeFieldName.split("\\.")[0];
					String attrName = completeFieldName.split("\\.")[1];
					List<Integer> posList = activeDb.getTable(tableName).getAttrPos(attrName);
					String allList;
					if (posList.get(0) == 0) {
						allList = entries.get(onePK);
					} else {
						allList = onePK;
					}
					String[] tableData = allList.split("\n");
					
					String[] splitList = ValidatorDictionary.partsHotfix(tableData[Integer.parseInt(tableMap.get(tableName).split(",")[1])].split(","));
					String value = splitList[posList.get(1)];
					thisAns.append(value);
					thisAns.append(",");
				}
			}
			
			if (distinct) {
				if (!distinctAnswerSet.contains(thisAns.toString())) {
					distinctAnswerSet.add(thisAns.toString());
					rowNr++;
					ans.append(thisAns.toString());
					ans.append("\n");
				}
			} else {
				rowNr++;
				ans.append(thisAns.toString());
				ans.append("\n");
			}
		} // end of thisGroupKey
		
		String headers = fieldAlias.stream().collect(Collectors.joining(","));
		System.out.println(rowNr + "\n" + colNr + "\n" + headers + "\n"+ ans.toString());
//		System.out.println("ColNr: " + colNr);
//		System.out.println("Headers: " + headers);
//		System.out.println(rowNr + "\n" + colNr + "\n" + headers + "\n" + ans.toString());
		return rowNr + "\n" + colNr + "\n" + headers + "\n"+ ans.toString();
	}
	
	private static String executeNoGroupAggrSelect(ValidatorDictionary valDict, Table activeTable, Database activeDb, Map<String, String> allDocuments, Map<String, String> tableMap, boolean distinct) {
		StringBuilder ans = new StringBuilder("");
		List<List<String>> retList = parseSelectParts(valDict, tableMap, activeDb);
		List<String> fieldNames = retList.get(0);
		List<String> fieldAlias = retList.get(1);
		Set<String> distinctAnswerSet = new HashSet<>();
		
		// check if ungrouped attribute appears in select list
		for (String thisFieldName : fieldNames) {
			if (!thisFieldName.contains("(")) {
				return "Error: Attribute " + thisFieldName + " in SELECT list is not in GROUP BY clause!";
			}
		}
		
		int rowNr = 0;
		int colNr = fieldNames.size();
		
		List<String> aggregateFields = new ArrayList<>();
		List<String> aggregateTypes = new ArrayList<>();
		for (String thisFieldName : fieldNames) {
			if (thisFieldName.contains("(")) {
				String aggregateField = thisFieldName.split("\\(")[1].split("\\)")[0];
				String aggregateType = thisFieldName.split("\\(")[0].toUpperCase();
				String aTable;
				String aField;
				if (aggregateField.contains(".")) {
					aTable = tableMap.get(aggregateField.split("\\.")[0]).split(",")[0];
					aField = aggregateField.split("\\.")[1];	
				} else {
					aTable = activeTable.getTableName();
					aField = aggregateField;
				}
				
				if (aTable == null || activeDb.getTable(aTable) == null || activeDb.getTable(aTable).getAttribute(aField) == null) {
					return "Error: Attribute " + aggregateField + " does not exist!";
				}
				
				boolean numericFunction = false;
				boolean numericData = false;
				if (aggregateType.equals("AVG") || aggregateType.equals("SUM") || aggregateType.equals("MIN") || aggregateType.equals("MAX")) {
					numericFunction = true;
				}
				String dataType = activeDb.getTable(aTable).getAttribute(aField).getAttributeType().toLowerCase();
				if (dataType.equals("float") || dataType.equals("double") || dataType.equals("int")) {
					numericData = true;
				}
				if (numericFunction && !numericData) {
					return "Error: " + aggregateType + "() is not defined for type " + dataType + "!";
				}
				
				aggregateFields.add(aTable + "." + aField);
				aggregateTypes.add(aggregateType);
			} else {
				return "Error: Non-aggregated field in SELECT clause without GROUP BY!";
			}
		}
		
		int aggrSize = aggregateFields.size();
		List<String> aggrValues = new ArrayList<>();
		for (int i=0; i<aggrSize; i++) {
			aggrValues.add("");
		}
		
		Map<String,String> entries = allDocuments;
		for (String thisPK : entries.keySet()) {
			String thisValue = entries.get(thisPK);
			
			for (int i=0; i<aggrSize; i++) {
				String tableName = aggregateFields.get(i).split("\\.")[0];
				String attrName = aggregateFields.get(i).split("\\.")[1];
				List<Integer> posList = activeDb.getTable(tableName).getAttrPos(attrName);
				String allList;
				if (posList.get(0) == 0) {
					allList = thisValue;
				} else {
					allList = thisPK;
				}
				String[] tableData = allList.split("\n");
				
				String[] splitList = ValidatorDictionary.partsHotfix(tableData[Integer.parseInt(tableMap.get(tableName).split(",")[1])].split(","));
				String value = splitList[posList.get(1)];
				if (value.equals("null")) {
					continue;
				}
				String type = activeDb.getTable(tableName).getAttribute(attrName).getAttributeType().toLowerCase();
				boolean numericType = false;
				if (type.equals("float") || type.equals("int") || type.equals("double")) {
					numericType = true;
				}
				
				switch (aggregateTypes.get(i).toUpperCase()) {
					case "SUM": {
						Double partialSum = 0.0;
						if (aggrValues.get(i).isEmpty()) {
							partialSum = 0.0;
						} else {
							partialSum = Double.parseDouble(aggrValues.get(i));
						}
						partialSum = partialSum + Double.parseDouble(value);
						aggrValues.set(i, partialSum.toString());
						break;
					}
					case "AVG": {
						Double partialSum = 0.0;
						Integer rowCount = 0;
						if (aggrValues.get(i).isEmpty()) {
							partialSum = 0.0;
							rowCount = 0;
						} else {
							partialSum = Double.parseDouble(aggrValues.get(i).split(",")[0]);
							rowCount = Integer.parseInt(aggrValues.get(i).split(",")[1]);
						}
						partialSum = partialSum + Double.parseDouble(value);
						rowCount = rowCount + 1;
						aggrValues.set(i, partialSum.toString() + "," + rowCount.toString());
						break;
					}
					case "COUNT": {
						Integer rowCount = 0;
						if (aggrValues.get(i).isEmpty()) {
							rowCount = 0;
						} else {
							rowCount = Integer.parseInt(aggrValues.get(i));
						}
						rowCount = rowCount + 1;
						aggrValues.set(i, rowCount.toString());
						break;
					}
					case "MIN": {
						if (numericType) {
							Double currentValue = Double.parseDouble(value);
							if (aggrValues.get(i).isEmpty()) {
								aggrValues.set(i, currentValue.toString());
							} else {
								Double oldValue = Double.parseDouble(aggrValues.get(i));
								Double minDouble = Math.min(oldValue, currentValue);
								aggrValues.set(i, minDouble.toString());
							}
						} else {
							if (aggrValues.get(i).isEmpty()) {
								aggrValues.set(i, value);
							} else {
								String oldValue = aggrValues.get(i);
								if (value.compareTo(oldValue) == -1) {
									aggrValues.set(i, value);
								}
							}
						}
						break;
					}
					case "MAX": {
						if (numericType) {
							Double currentValue = Double.parseDouble(value);
							if (aggrValues.get(i).isEmpty()) {
								aggrValues.set(i, currentValue.toString());
							} else {
								Double oldValue = Double.parseDouble(aggrValues.get(i));
								Double minDouble = Math.max(oldValue, currentValue);
								aggrValues.set(i, minDouble.toString());
							}
						} else {
							if (aggrValues.get(i).isEmpty()) {
								aggrValues.set(i, value);
							} else {
								String oldValue = aggrValues.get(i);
								if (value.compareTo(oldValue) == 1) {
									aggrValues.set(i, value);
								}
							}
						}
						break;
					}
				}
			}
			
		} // end of thisPK
			
		for (int i=0; i<aggrSize; i++) {
			if (aggrValues.get(i).isEmpty()) {
				if (aggregateTypes.get(i).toUpperCase().equals("COUNT")) {
					aggrValues.set(i, "0");
				} else {
					aggrValues.set(i, "null");
				}
			} else if (aggregateTypes.get(i).toUpperCase().equals("AVG")) {
				Double sum = Double.parseDouble(aggrValues.get(i).split(",")[0]);
				Double count = Double.parseDouble(aggrValues.get(i).split(",")[1]);
				Double avg = sum/count;
				aggrValues.set(i, avg.toString());
			}
		}
		
		int aggrIterInd = 0;
		StringBuilder thisAns = new StringBuilder("");
		for (String thisFieldName : fieldNames) {
			if (thisFieldName.contains("(")) {
				thisAns.append(aggrValues.get(aggrIterInd));
				thisAns.append(",");
				aggrIterInd += 1;
			}
		}
		if (distinct) {
			if (!distinctAnswerSet.contains(thisAns.toString())) {
				distinctAnswerSet.add(thisAns.toString());
				rowNr++;
				ans.append(thisAns.toString());
				ans.append("\n");
			}
		} else {
			rowNr++;
			ans.append(thisAns.toString());
			ans.append("\n");
		}
		
		String headers = fieldAlias.stream().collect(Collectors.joining(","));
		System.out.println(rowNr + "\n" + colNr + "\n" + headers + "\n"+ ans.toString());
//		System.out.println("ColNr: " + colNr);
//		System.out.println("Headers: " + headers);
//		System.out.println(rowNr + "\n" + colNr + "\n" + headers + "\n" + ans.toString());
		return rowNr + "\n" + colNr + "\n" + headers + "\n"+ ans.toString();
	}
	
	private static String verifyTableField(String[] parts, int ind, Map<String, String> tableMap, Database activeDb, Table fromTable) {
		if (parts.length > ind + 2 && parts[ind+1].equals(".")) {
			String tableName = parts[ind];
			String fieldName = parts[ind+2];
			if (!tableMap.containsKey(tableName)) {
				return "Error";
			}
			Table ownTable = activeDb.getTable(tableMap.get(tableName).split(",")[0]);
			if (ownTable.getAttribute(fieldName) == null) {
				return "Error";
			}
			return tableMap.get(tableName).split(",")[0] + "," + fieldName + ",3";
		} else {
			Table ownTable = fromTable;
			String fieldName = parts[ind];
			if (ownTable.getAttribute(fieldName) == null) {
				return "Error";
			}
			return tableMap.get(fromTable.getTableName()).split(",")[0] + "," + fieldName + ",1";
		}
	}
	
	
	private static List<List<String>> parseSelectParts(ValidatorDictionary valDict, Map<String, String> tableMap, Database activeDb) {
		List<String> fieldNames = new ArrayList<String>();
		List<String> fieldAlias = new ArrayList<String>();
		List<String> selectParts = valDict.getSelectParts();
		List<String> activeParts = new ArrayList<String>();
		
		if (selectParts.get(0).equals("* ")) {

			int stop = tableMap.values().stream().collect(Collectors.toSet()).size();
			for (int i = 0; i < stop; i++) {
				
				for (String oneTable : tableMap.keySet()) {
					String[] tableNameNr = tableMap.get(oneTable).split(",");
					if (Integer.parseInt(tableNameNr[1]) == i) {
						Table thisTable = activeDb.getTable(tableNameNr[0]);
						if (thisTable == null) {
							return null;
						}
						for (Attribute attr : thisTable.getAttributes()) {
							fieldNames.add(tableNameNr[0] + "." + attr.getAttributeName());
							fieldAlias.add(tableNameNr[0] + "." + attr.getAttributeName());
						}
						break;
					}
				}
			}
		} else {
			for (String thisPart : selectParts) {
				if (thisPart.isEmpty()) {
					if (activeParts.size() != 0) {
						addFieldsAndAlias(fieldNames, fieldAlias, activeParts);
						activeParts = new ArrayList<String>();
					}
				} else {
					activeParts.add(thisPart);
				}
			}
			addFieldsAndAlias(fieldNames, fieldAlias, activeParts);
		}
		
		List<List<String>> retList = new ArrayList<>();
		retList.add(fieldNames);
		retList.add(fieldAlias);
		return retList;
	}
	
	
	private static void addFieldsAndAlias(List<String> fieldNames, List<String> fieldAlias, List<String> activeParts) {
		int len = activeParts.size();
		
		// this is an aggregate
		if (len > 1 && activeParts.get(1).equals("(")) {
			String fullName = activeParts.stream().collect(Collectors.joining());
			fieldNames.add(fullName);
			
			// check if has alias
			if (!activeParts.get(len-1).equals(")")) {
				fieldAlias.add(activeParts.get(len-1));
			} else {
				fieldAlias.add(fullName);
			}
			return;
		}
		
		// not an aggregate
		switch (len) {
			case 1: {
				// SELECT fieldName
				fieldNames.add(activeParts.get(0));
				fieldAlias.add(activeParts.get(0));
				break;
			}
			case 2: {
				// SELECT fieldName alias
				fieldNames.add(activeParts.get(0));
				fieldAlias.add(activeParts.get(1));
				break;
			}
			case 3: {
				if (activeParts.get(1).equals(".")) {
					// SELECT table.fieldName
					fieldNames.add(activeParts.get(0) + "." + activeParts.get(2));
					fieldAlias.add(activeParts.get(0) + "." + activeParts.get(2));
				} else {
					// SELECT fieldName AS alias
					fieldNames.add(activeParts.get(0));
					fieldAlias.add(activeParts.get(2));
				}
				break;
			}
			case 4: {
				// SELECT table.fieldName alias
				fieldNames.add(activeParts.get(0) + "." + activeParts.get(2));
				fieldAlias.add(activeParts.get(3));
				break;
			}
			case 5: {
				// SELECT table.fieldName AS alias
				fieldNames.add(activeParts.get(0) + "." + activeParts.get(2));
				fieldAlias.add(activeParts.get(4));
				break;
			}
		}
	}
	
}
