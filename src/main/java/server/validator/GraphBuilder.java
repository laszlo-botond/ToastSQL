package server.validator;

public class GraphBuilder {
	public static void addCreateDropNodes(SQLCommand validatorRoot) {
		// CREATE TABLE branch
        SQLCommand createTable = new SQLCommand(false);
        createTable.addAcceptablePhrase("CREATE TABLE ");

        SQLCommand tableName = new SQLCommand(true);

        SQLCommand createTableOpener = new SQLCommand(false);
        createTableOpener.addAcceptablePhrase("(");

        SQLCommand createTableCloser = new SQLCommand(false);
        createTableCloser.addAcceptablePhrase(")");
        createTableCloser.setOkAsLast();

        SQLCommand separatorComma = new SQLCommand(false);
        separatorComma.addAcceptablePhrase(",");

        // Field name + type
            SQLCommand fieldName = new SQLCommand(true);
            fieldName.setAsCustomRequest("CUSTOM_FIELD,");

            SQLCommand fieldType = new SQLCommand(false);
            fieldType.addAcceptablePhrase("INT");
            fieldType.addAcceptablePhrase("FLOAT");
            fieldType.addAcceptablePhrase("DOUBLE");
            fieldType.addAcceptablePhrase("DATE");
            fieldType.addAcceptablePhrase("DATETIME");
            fieldType.addAcceptablePhrase("BOOL");
            
            SQLCommand fieldTypeSpace = new SQLCommand(false);
            fieldTypeSpace.addAcceptablePhrase("INT ");
            fieldTypeSpace.addAcceptablePhrase("FLOAT ");
            fieldTypeSpace.addAcceptablePhrase("DOUBLE ");
            fieldTypeSpace.addAcceptablePhrase("DATE ");
            fieldTypeSpace.addAcceptablePhrase("DATETIME ");
            fieldType.addAcceptablePhrase("BOOL ");

            SQLCommand charType = new SQLCommand(false);
            charType.addAcceptablePhrase("CHAR");
            charType.addAcceptablePhrase("VARCHAR");

            SQLCommand charLengthOpener = new SQLCommand(false);
            charLengthOpener.addAcceptablePhrase("(");

            SQLCommand charLength = new SQLCommand(false);
            charLength.needsNumber();

            SQLCommand charLengthCloser = new SQLCommand(false);
            charLengthCloser.addAcceptablePhrase(")");
            
            SQLCommand charLengthCloserSpace = new SQLCommand(false);
            charLengthCloserSpace.addAcceptablePhrase(") ");

        // Index
            SQLCommand index = new SQLCommand(false);
            index.addAcceptablePhrase("INDEX ");
            index.addAcceptablePhrase("UNIQUE INDEX ");

            SQLCommand indexName = new SQLCommand(true);

            SQLCommand indexOpener = new SQLCommand(false);
            indexOpener.addAcceptablePhrase("(");

            SQLCommand indexCloser = new SQLCommand(false);
            indexCloser.addAcceptablePhrase(")");

            SQLCommand indexField = new SQLCommand(true);

            SQLCommand indexFieldSep = new SQLCommand(false);
            indexFieldSep.addAcceptablePhrase(",");

        // Primary key
            SQLCommand primaryKey = new SQLCommand(false);
            primaryKey.addAcceptablePhrase("PRIMARY KEY");

            SQLCommand primaryKeyOpener = new SQLCommand(false);
            primaryKeyOpener.addAcceptablePhrase("(");

            SQLCommand primaryKeyFieldName = new SQLCommand(true);

            SQLCommand primaryKeyFieldSep = new SQLCommand(false);
            primaryKeyFieldSep.addAcceptablePhrase(",");

            SQLCommand primaryKeyCloser = new SQLCommand(false);
            primaryKeyCloser.addAcceptablePhrase(")");

        // Foreign key
            SQLCommand fk = new SQLCommand(false);
            fk.addAcceptablePhrase("FOREIGN KEY");

            SQLCommand fkOpener = new SQLCommand(false);
            fkOpener.addAcceptablePhrase("(");

            SQLCommand fkFieldName = new SQLCommand(true);

            SQLCommand fkFieldSep = new SQLCommand(false);
            fkFieldSep.addAcceptablePhrase(",");

            SQLCommand fkCloser = new SQLCommand(false);
            fkCloser.addAcceptablePhrase(")");

            SQLCommand fkReferences = new SQLCommand(false);
            fkReferences.addAcceptablePhrase("REFERENCES ");

            SQLCommand fkRefTable = new SQLCommand(true);

            SQLCommand fkRefTableOpener = new SQLCommand(false);
            fkRefTableOpener.addAcceptablePhrase("(");

            SQLCommand fkRefTableFieldName = new SQLCommand(true);

            SQLCommand fkRefTableFieldSep = new SQLCommand(false);
            fkRefTableFieldSep.addAcceptablePhrase(",");

            SQLCommand fkRefTableCloser = new SQLCommand(false);
            fkRefTableCloser.addAcceptablePhrase(")");

        // Unique constraint
            SQLCommand uniq = new SQLCommand(false);
            uniq.addAcceptablePhrase("UNIQUE");

            SQLCommand uniqOpener = new SQLCommand(false);
            uniqOpener.addAcceptablePhrase("(");

            SQLCommand uniqFieldName = new SQLCommand(true);

            SQLCommand uniqFieldSep = new SQLCommand(false);
            uniqFieldSep.addAcceptablePhrase(",");

            SQLCommand uniqCloser = new SQLCommand(false);
            uniqCloser.addAcceptablePhrase(")");

        // Constraint
            SQLCommand cnstr = new SQLCommand(false);
            cnstr.addAcceptablePhrase("CONSTRAINT ");

            SQLCommand cnstrName = new SQLCommand(true);

        // Field extras
            SQLCommand fieldUniq = new SQLCommand(false);
            fieldUniq.addAcceptablePhrase("UNIQUE");

            SQLCommand fieldPK = new SQLCommand(false);
            fieldPK.addAcceptablePhrase("PRIMARY KEY");

            SQLCommand fieldNull = new SQLCommand(false);
            fieldNull.addAcceptablePhrase("NULL");

            SQLCommand fieldNotNull = new SQLCommand(false);
            fieldNotNull.addAcceptablePhrase("NOT NULL");
            
            SQLCommand fieldNotNullSpace = new SQLCommand(false);
            fieldNotNullSpace.addAcceptablePhrase("NOT NULL ");

            SQLCommand fieldDefault = new SQLCommand(false);
            fieldDefault.addAcceptablePhrase("DEFAULT ");

            SQLCommand fieldDefaultValue = new SQLCommand(true);
            fieldDefaultValue.allowString();


        // ----- Tree hierarchy -----
            validatorRoot.addChild(createTable);
            createTable.addChild(tableName);
            tableName.addChild(createTableOpener);

            // Field name and type
            createTableOpener.addChild(fieldName);
            separatorComma.addChild(fieldName);

            fieldName.addChild(fieldTypeSpace);
            fieldName.addChild(fieldType);
            fieldName.addChild(charType);
            charType.addChild(charLengthOpener);
            fieldType.addChild(separatorComma); // new block after correct fieldType declaration
            fieldType.addChild(createTableCloser); // can close after correct fieldType declaration
            charLengthOpener.addChild(charLength);
            charLength.addChild(charLengthCloserSpace);
            charLength.addChild(charLengthCloser);
            charLengthCloser.addChild(separatorComma); // new block after correct charType(length)
            charLengthCloser.addChild(createTableCloser); // can close after correct charType(length)

            // Index
            separatorComma.addChild(index);

            index.addChild(indexName);
            indexName.addChild(indexOpener);
            indexOpener.addChild(indexField);
            indexField.addChild(indexCloser);
            indexField.addChild(indexFieldSep);
            indexFieldSep.addChild(indexField);
            indexCloser.addChild(separatorComma); // new block after correct index declaration
            indexCloser.addChild(createTableCloser); // can close after correct index declaration

            // Primary key
            separatorComma.addChild(primaryKey);

            primaryKey.addChild(primaryKeyOpener);
            primaryKeyOpener.addChild(primaryKeyFieldName);
            primaryKeyFieldName.addChild(primaryKeyFieldSep);
            primaryKeyFieldName.addChild(primaryKeyCloser);
            primaryKeyFieldSep.addChild(primaryKeyFieldName);
            primaryKeyCloser.addChild(separatorComma); // new block after correct PK declaration
            primaryKeyCloser.addChild(createTableCloser); // can close after correct PK declaration

            // Foreign key
            separatorComma.addChild(fk);

            fk.addChild(fkOpener);
            fkOpener.addChild(fkFieldName);
            fkFieldName.addChild(fkFieldSep);
            fkFieldSep.addChild(fkFieldName);
            fkFieldName.addChild(fkCloser);
            fkCloser.addChild(fkReferences);
            fkReferences.addChild(fkRefTable);
            fkRefTable.addChild(fkRefTableOpener);
            fkRefTableOpener.addChild(fkRefTableFieldName);
            fkRefTableFieldName.addChild(fkRefTableFieldSep);
            fkRefTableFieldSep.addChild(fkRefTableFieldName);
            fkRefTableFieldName.addChild(fkRefTableCloser);
            fkRefTableCloser.addChild(separatorComma); // new block after correct FK declaration
            fkRefTableCloser.addChild(createTableCloser); // can close after correct FK declaration

            // Unique
            separatorComma.addChild(uniq);

            uniq.addChild(uniqOpener);
            uniqOpener.addChild(uniqFieldName);
            uniqFieldName.addChild(uniqFieldSep);
            uniqFieldSep.addChild(uniqFieldName);
            uniqFieldName.addChild(uniqCloser);
            uniqCloser.addChild(separatorComma); // new block after correct UNIQUE clause
            uniqCloser.addChild(createTableCloser); // can close after correct UNIQUE declaration

            // Constraint
            separatorComma.addChild(cnstr);

            cnstr.addChild(cnstrName);
            cnstrName.addChild(fk);
            cnstrName.addChild(primaryKey);
            cnstrName.addChild(uniq);

            // Field extras
            fieldTypeSpace.addChild(fieldPK);
            fieldTypeSpace.addChild(fieldNull);
            fieldTypeSpace.addChild(fieldNotNullSpace); // at charLengthCloser too
            fieldTypeSpace.addChild(fieldNotNull); // at charLengthCloser too
            fieldNotNullSpace.addChild(fieldDefault);
            fieldTypeSpace.addChild(fieldDefault);
            fieldTypeSpace.addChild(fieldUniq);

            fieldDefault.addChild(fieldDefaultValue);

            fieldPK.addChild(separatorComma);
            fieldNull.addChild(separatorComma);
            fieldDefaultValue.addChild(separatorComma);
            fieldUniq.addChild(separatorComma);
            fieldNotNull.addChild(separatorComma);

            fieldPK.addChild(createTableCloser);
            fieldNull.addChild(createTableCloser);
            fieldDefaultValue.addChild(createTableCloser);
            fieldUniq.addChild(createTableCloser);
            fieldNotNull.addChild(createTableCloser);

            charLengthCloserSpace.addChild(fieldPK);
            charLengthCloserSpace.addChild(fieldNull);
            charLengthCloserSpace.addChild(fieldDefault);
            charLengthCloserSpace.addChild(fieldUniq);
            charLengthCloserSpace.addChild(fieldNotNullSpace);
            charLengthCloserSpace.addChild(fieldNotNull);


        // CREATE DATABASE branch
        SQLCommand createDB = new SQLCommand(false);
        createDB.addAcceptablePhrase("CREATE DATABASE ");

        SQLCommand createDBName = new SQLCommand(true);
        createDBName.setOkAsLast();

        // Tree hierarchy
        validatorRoot.addChild(createDB);
        createDB.addChild(createDBName);



        // DROP DATABASE branch
        SQLCommand dropDB = new SQLCommand(false);
        dropDB.addAcceptablePhrase("DROP DATABASE ");

        SQLCommand dropDBName = new SQLCommand(true);
        dropDBName.setOkAsLast();

        // Tree hierarchy
        validatorRoot.addChild(dropDB);
        dropDB.addChild(dropDBName);



        // DROP TABLE branch
        SQLCommand dropTable = new SQLCommand(false);
        dropTable.addAcceptablePhrase("DROP TABLE ");

        SQLCommand dropTableName = new SQLCommand(true);
        dropTableName.setOkAsLast();

        // Tree hierarchy
        validatorRoot.addChild(dropTable);
        dropTable.addChild(dropTableName); // drop table can now only be last command



        // CREATE INDEX branch
        SQLCommand createIndex = new SQLCommand(false);
        createIndex.addAcceptablePhrase("CREATE INDEX ");
        createIndex.addAcceptablePhrase("CREATE UNIQUE INDEX ");

        SQLCommand createIndexName = new SQLCommand(true);

        SQLCommand createIndexOn = new SQLCommand(false);
        createIndexOn.addAcceptablePhrase("ON ");

        SQLCommand createIndexTable = new SQLCommand(true);

        SQLCommand createIndexFieldOpener = new SQLCommand(false);
        createIndexFieldOpener.addAcceptablePhrase("(");

        SQLCommand createIndexField = new SQLCommand(true);

        SQLCommand createIndexFieldSep = new SQLCommand(false);
        createIndexFieldSep.addAcceptablePhrase(",");

        SQLCommand createIndexFieldCloser = new SQLCommand(false);
        createIndexFieldCloser.addAcceptablePhrase(")");
        createIndexFieldCloser.setOkAsLast();

        // Tree hierarchy
        validatorRoot.addChild(createIndex);
        createIndex.addChild(createIndexName);
        createIndexName.addChild(createIndexOn);
        createIndexOn.addChild(createIndexTable);
        createIndexTable.addChild(createIndexFieldOpener);
        createIndexFieldOpener.addChild(createIndexField);
        createIndexField.addChild(createIndexFieldSep);
        createIndexFieldSep.addChild(createIndexField);
        createIndexField.addChild(createIndexFieldCloser);



        // USE branch
        SQLCommand use = new SQLCommand(false);
        use.addAcceptablePhrase("USE ");

        SQLCommand useDB = new SQLCommand(true);
        useDB.setOkAsLast();
        useDB.setAsSeparator();

        // Tree hierarchy
        validatorRoot.addChild(use);
        use.addChild(useDB);
        useDB.addChild(validatorRoot);

        // Extras
        separatorComma.setAsSeparator();

        createTableCloser.setAsSeparator();
        createTableCloser.addChild(validatorRoot);

        createIndexFieldCloser.setAsSeparator();
        createIndexFieldCloser.addChild(validatorRoot);

        dropDBName.setAsSeparator();
        // dropDBName.addChild(validatorRoot); // drop DB can only be last now

        dropTableName.setAsSeparator();
        // dropTableName.addChild(validatorRoot); // drop table can only be last now

        createDBName.setAsSeparator();
        createDBName.addChild(validatorRoot);

        createTableOpener.setAsSeparator();
        
        // End bracket of create table fix
        fieldType.setAsSeparator();
        charLengthCloser.setAsSeparator();
        fieldUniq.setAsSeparator();
        fieldPK.setAsSeparator();
        fieldNull.setAsSeparator();
        fieldNotNull.setAsSeparator();
        fieldDefaultValue.setAsSeparator();
        indexCloser.setAsSeparator();
        primaryKeyCloser.setAsSeparator();
        fkRefTableCloser.setAsSeparator();
        uniqCloser.setAsSeparator();
        
        createTableCloser.setAsCustomRequest("TABLE_CREATED");
	}
	
	public static void addInsertNodes(SQLCommand validatorRoot) {
		// INSERT INTO
        SQLCommand insertInto = new SQLCommand(false);
        insertInto.addAcceptablePhrase("INSERT INTO ");
        
        SQLCommand insertTable = new SQLCommand(true);
        
        SQLCommand insertFieldOpener = new SQLCommand(false);
        insertFieldOpener.addAcceptablePhrase("(");
        
        SQLCommand insertField = new SQLCommand(true);
        
        SQLCommand insertFieldSep = new SQLCommand(false);
        insertFieldSep.addAcceptablePhrase(",");
        
        SQLCommand insertFieldCloser = new SQLCommand(false);
        insertFieldCloser.addAcceptablePhrase(")");
        insertFieldCloser.setAsSeparator();
        
        SQLCommand insertValues = new SQLCommand(false);
        insertValues.addAcceptablePhrase("VALUES");
        insertValues.setAsSeparator();
        
        SQLCommand insertValuesOpener = new SQLCommand(false);
        insertValuesOpener.addAcceptablePhrase("(");
        insertValuesOpener.setAsCustomRequest("CUSTOM_VALUES,");
        
        SQLCommand insertValuesVal = new SQLCommand(true);
        insertValuesVal.allowString();
        insertValuesVal.allowDot();
        
        SQLCommand insertValuesSep = new SQLCommand(false);
        insertValuesSep.addAcceptablePhrase(",");
        
        SQLCommand insertValuesCloser = new SQLCommand(false);
        insertValuesCloser.addAcceptablePhrase(")");
        insertValuesCloser.setOkAsLast();
        insertValuesCloser.setAsSeparator();
        
        SQLCommand insertValuesLineSep = new SQLCommand(false);
        insertValuesLineSep.addAcceptablePhrase(",");
        
        // Tree / Graph
        validatorRoot.addChild(insertInto);
        insertInto.addChild(insertTable);
        insertTable.addChild(insertFieldOpener);
        insertFieldOpener.addChild(insertField);
        insertField.addChild(insertFieldSep);
        insertField.addChild(insertFieldCloser);
        insertFieldSep.addChild(insertField);
        insertFieldCloser.addChild(insertValues);
        insertValues.addChild(insertValuesOpener);
        insertValuesOpener.addChild(insertValuesVal);
        insertValuesVal.addChild(insertValuesSep);
        insertValuesVal.addChild(insertValuesCloser);
        insertValuesSep.addChild(insertValuesVal);
        insertValuesCloser.addChild(insertValuesLineSep);
        insertValuesLineSep.addChild(insertValuesOpener);
        insertValuesCloser.addChild(validatorRoot);
	}
	
	public static void addDeleteNodes(SQLCommand validatorRoot) {
		// DELETE FROM t WHERE x op y
		SQLCommand deleteFrom = new SQLCommand(false);
		deleteFrom.addAcceptablePhrase("DELETE FROM ");
		
		SQLCommand deleteTable = new SQLCommand(true);
		
		SQLCommand deleteWhere = new SQLCommand(false);
		deleteWhere.addAcceptablePhrase("WHERE ");
		
		SQLCommand deleteLeft = new SQLCommand(true);
		
		SQLCommand deleteOp = new SQLCommand(false);
		deleteOp.addAcceptablePhrase("=");
		// deleteOp.addAcceptablePhrase("<");
		// deleteOp.addAcceptablePhrase(">");
		// deleteOp.addAcceptablePhrase("<=");
		// deleteOp.addAcceptablePhrase(">=");
		
		SQLCommand deleteRight = new SQLCommand(true);
		deleteRight.allowString();
		deleteRight.allowDot();
		deleteRight.setForceRedirect();
		
		SQLCommand logicalOperator = new SQLCommand(false);
		logicalOperator.addAcceptablePhrase("AND ");
		
		SQLCommand deleteEnder = new SQLCommand(false);
		deleteEnder.addAcceptablePhrase("");
		deleteEnder.setAsSeparator();
		deleteEnder.setOkAsLast();
		
		// Tree / Graph
		validatorRoot.addChild(deleteFrom);
		deleteFrom.addChild(deleteTable);
		deleteTable.addChild(deleteWhere);
		deleteWhere.addChild(deleteLeft);
		deleteLeft.addChild(deleteOp);
		deleteOp.addChild(deleteRight);
		deleteRight.addChild(deleteEnder);
		deleteRight.addChild(logicalOperator);
		logicalOperator.addChild(deleteLeft);
		
		deleteEnder.addChild(validatorRoot);
	}
	
	public static void addShowNodes(SQLCommand validatorRoot) {
		// SHOW TABLES / SHOW DATABASES
		SQLCommand showTables = new SQLCommand(false);
		showTables.addAcceptablePhrase("SHOW TABLES ");
		showTables.setOkAsLast();
		showTables.setAsSeparator();
		
		SQLCommand showDB = new SQLCommand(false);
		showDB.addAcceptablePhrase("SHOW DATABASES ");
		showDB.setOkAsLast();
		showDB.setAsSeparator();
		
		// Tree / Graph
		validatorRoot.addChild(showTables);
		showTables.addChild(validatorRoot);
		
		validatorRoot.addChild(showDB);
		showDB.addChild(validatorRoot);
	}
	
	public static void addSelectNodes(SQLCommand validatorRoot) {
		SQLCommand select = new SQLCommand(false);
		select.addAcceptablePhrase("SELECT DISTINCT ");
		select.addAcceptablePhrase("SELECT ");
		
		// selected fields
		SQLCommand starSelectAll = new SQLCommand(false);
		starSelectAll.addAcceptablePhrase("* ");
		
		SQLCommand selectTableName = new SQLCommand(true);
		
		SQLCommand selectTableDot = new SQLCommand(false);
		selectTableDot.addAcceptablePhrase(".");
		
		SQLCommand selectFieldName = new SQLCommand(true);
		
		SQLCommand selectFieldNameAs = new SQLCommand(false);
		selectFieldNameAs.addAcceptablePhrase("AS ");
		
		SQLCommand selectFieldAbbreviation = new SQLCommand(true);
		
		SQLCommand selectFieldSepComma = new SQLCommand(false);
		selectFieldSepComma.addAcceptablePhrase(",");
		
		// aggregate selection
		SQLCommand aggregateFunction = new SQLCommand(false);
		aggregateFunction.addAcceptablePhrase("COUNT");
		aggregateFunction.addAcceptablePhrase("MIN");
		aggregateFunction.addAcceptablePhrase("MAX");
		aggregateFunction.addAcceptablePhrase("SUM");
		aggregateFunction.addAcceptablePhrase("AVG");
		
		SQLCommand aggregateOpener = new SQLCommand(false);
		aggregateOpener.addAcceptablePhrase("(");
		
		SQLCommand aggrTableName = new SQLCommand(true);
		
		SQLCommand aggrTableDot = new SQLCommand(false);
		aggrTableDot.addAcceptablePhrase(".");
		
		SQLCommand aggrFieldName = new SQLCommand(true);
		
		SQLCommand aggregateCloser = new SQLCommand(false);
		aggregateCloser.addAcceptablePhrase(")");
		
		SQLCommand aggrAs = new SQLCommand(false);
		aggrAs.addAcceptablePhrase("AS ");
		
		SQLCommand aggrAlias = new SQLCommand(true);
		
		// from
		SQLCommand sqlFrom = new SQLCommand(false);
		sqlFrom.addAcceptablePhrase("FROM ");
		
		SQLCommand fromTableName = new SQLCommand(true);
		fromTableName.setAsSeparator();
		fromTableName.setForceRedirect();
		
		SQLCommand fromTableAs = new SQLCommand(false);
		fromTableAs.setAsSeparator();
		fromTableAs.addAcceptablePhrase("AS ");
		fromTableAs.setAsCustomRequest("FROM_TABLE_AS ,");
		
		SQLCommand fromTableAbbreviation = new SQLCommand(true);
		fromTableAbbreviation.setAsSeparator();
		fromTableAbbreviation.setAsCustomRequest("FROM_TABLE_ABBREVIATION ,");
		fromTableAbbreviation.setForceRedirect();
		
		// join
		SQLCommand sqlJoin = new SQLCommand(false);
		sqlJoin.addAcceptablePhrase("JOIN ");
		sqlJoin.addAcceptablePhrase("INNER JOIN ");
		
		SQLCommand joinTableName = new SQLCommand(true);
		
		SQLCommand joinTableAs = new SQLCommand(false);
		joinTableAs.addAcceptablePhrase("AS ");
		
		SQLCommand joinTableAbbreviation = new SQLCommand(true);
		
		SQLCommand joinOn = new SQLCommand(false);
		joinOn.addAcceptablePhrase("ON ");
		
		SQLCommand joinCondTable1 = new SQLCommand(true);
		
		SQLCommand joinTable1Dot = new SQLCommand(false);
		joinTable1Dot.addAcceptablePhrase(".");
		
		SQLCommand joinField1 = new SQLCommand(true);
		
		SQLCommand joinOperator = new SQLCommand(false);
		joinOperator.addAcceptablePhrase("=");
		
		SQLCommand joinCondTable2 = new SQLCommand(true);
		
		SQLCommand joinTable2Dot = new SQLCommand(false);
		joinTable2Dot.addAcceptablePhrase(".");
		
		SQLCommand joinField2 = new SQLCommand(true);
		joinField2.setAsSeparator();
		joinField2.setForceRedirect();
		
		// where
		SQLCommand sqlWhere = new SQLCommand(false);
		sqlWhere.addAcceptablePhrase("WHERE ");
		
		SQLCommand condTable1 = new SQLCommand(true);
		
		SQLCommand condTable1Dot = new SQLCommand(false);
		condTable1Dot.addAcceptablePhrase(".");
		
		SQLCommand condField1 = new SQLCommand(true);
		condField1.allowString();
		
		SQLCommand whereOperator = new SQLCommand(false);
		whereOperator.addAcceptablePhrase("<=");
		whereOperator.addAcceptablePhrase(">=");
		whereOperator.addAcceptablePhrase("<>");
		whereOperator.addAcceptablePhrase("<");
		whereOperator.addAcceptablePhrase(">");
		whereOperator.addAcceptablePhrase("=");
		
		SQLCommand condTable2 = new SQLCommand(true);
		
		SQLCommand condTable2Dot = new SQLCommand(false);
		condTable2Dot.addAcceptablePhrase(".");
		
		SQLCommand condField2 = new SQLCommand(true);
		condField2.setAsSeparator();
		condField2.allowString();
		condField2.setForceRedirect();
		
		SQLCommand whereAnd = new SQLCommand(false);
		whereAnd.addAcceptablePhrase("AND ");
		whereAnd.setAsCustomRequest("WHERE_");
		
		// group by
		SQLCommand groupBy = new SQLCommand(false);
		groupBy.addAcceptablePhrase("GROUP BY ");
		
		SQLCommand groupByTable = new SQLCommand(true);
		
		SQLCommand groupByDot = new SQLCommand(false);
		groupByDot.addAcceptablePhrase(".");
		
		SQLCommand groupByField = new SQLCommand(true);
		groupByField.setForceRedirect();
		
		SQLCommand groupByComma = new SQLCommand(false);
		groupByComma.addAcceptablePhrase(",");
		
		SQLCommand groupByEnder = new SQLCommand(false);
		groupByEnder.addAcceptablePhrase("");
		groupByEnder.setAsSeparator();
		groupByEnder.setOkAsLast();
		
		// end
		SQLCommand selectEnder = new SQLCommand(false);
		selectEnder.addAcceptablePhrase("");
		selectEnder.setOkAsLast();
		selectEnder.setAsSeparator();
		selectEnder.setAsCustomRequest("SELECT_ENDED ");
		
		// Graph
		validatorRoot.addChild(select);
		
		select.addChild(starSelectAll);
		select.addChild(aggregateFunction);
		select.addChild(selectTableName);
		select.addChild(selectFieldName);
		
		starSelectAll.addChild(sqlFrom);
		
		aggregateFunction.addChild(aggregateOpener);
		
		aggregateOpener.addChild(aggrFieldName);
		aggregateOpener.addChild(aggrTableName);
		
		aggrTableName.addChild(aggrTableDot);
		
		aggrTableDot.addChild(aggrFieldName);
		
		aggrFieldName.addChild(aggregateCloser);
		
		aggregateCloser.addChild(sqlFrom);
		aggregateCloser.addChild(selectFieldSepComma);
		aggregateCloser.addChild(aggrAs);
		aggregateCloser.addChild(aggrAlias);
		
		aggrAs.addChild(aggrAlias);
		
		aggrAlias.addChild(sqlFrom);
		aggrAlias.addChild(selectFieldSepComma);
		
		selectTableName.addChild(selectTableDot);
		
		selectTableDot.addChild(selectFieldName);
		
		selectFieldName.addChild(sqlFrom);
		selectFieldName.addChild(selectFieldNameAs);
		selectFieldName.addChild(selectFieldAbbreviation);
		selectFieldName.addChild(selectFieldSepComma);
		
		selectFieldNameAs.addChild(selectFieldAbbreviation);
		
		selectFieldAbbreviation.addChild(selectFieldSepComma);
		selectFieldAbbreviation.addChild(sqlFrom);
		
		selectFieldSepComma.addChild(aggregateFunction);
		selectFieldSepComma.addChild(selectFieldName);
		selectFieldSepComma.addChild(selectTableName);
		
		sqlFrom.addChild(fromTableName);
		
		fromTableName.addChild(fromTableAs);
		fromTableName.addChild(fromTableAbbreviation);
		fromTableName.addChild(sqlJoin);
		fromTableName.addChild(sqlWhere);
		fromTableName.addChild(selectEnder);
		fromTableName.addChild(groupBy);
		
		fromTableAs.addChild(fromTableAbbreviation);
		
		fromTableAbbreviation.addChild(sqlWhere);
		fromTableAbbreviation.addChild(sqlJoin);
		fromTableAbbreviation.addChild(selectEnder);
		fromTableAbbreviation.addChild(groupBy);
		
		sqlJoin.addChild(joinTableName);
		
		joinTableName.addChild(joinOn);
		joinTableName.addChild(joinTableAs);
		joinTableName.addChild(joinTableAbbreviation);
		
		joinTableAs.addChild(joinTableAbbreviation);
		
		joinTableAbbreviation.addChild(joinOn);
		
		joinOn.addChild(joinCondTable1);
		joinOn.addChild(joinField1);
		
		joinCondTable1.addChild(joinTable1Dot);
		
		joinTable1Dot.addChild(joinField1);
		
		joinField1.addChild(joinOperator);
		
		joinOperator.addChild(joinCondTable2);
		joinOperator.addChild(joinField2);
		
		joinCondTable2.addChild(joinTable2Dot);
		
		joinTable2Dot.addChild(joinField2);
		
		joinField2.addChild(sqlJoin);
		joinField2.addChild(sqlWhere);
		joinField2.addChild(selectEnder);
		joinField2.addChild(groupBy);
		
		sqlWhere.addChild(condTable1);
		sqlWhere.addChild(condField1);
		
		condTable1.addChild(condTable1Dot);
		
		condTable1Dot.addChild(condField1);
		
		condField1.addChild(whereOperator);
		
		whereOperator.addChild(condTable2);
		whereOperator.addChild(condField2);
		
		condTable2.addChild(condTable2Dot);
		
		condTable2Dot.addChild(condField2);
		
		condField2.addChild(whereAnd);
		condField2.addChild(selectEnder);
		condField2.addChild(groupBy);
		
		whereAnd.addChild(condTable1);
		whereAnd.addChild(condField1);
		
		groupBy.addChild(groupByTable);
		groupBy.addChild(groupByField);
		
		groupByTable.addChild(groupByDot);
		
		groupByDot.addChild(groupByField);
		
		groupByField.addChild(groupByComma);
		groupByField.addChild(groupByEnder);
		
		groupByComma.addChild(groupByTable);
		groupByComma.addChild(groupByField);
		
		groupByEnder.addChild(validatorRoot);
		
		selectEnder.addChild(validatorRoot);
	}
}
