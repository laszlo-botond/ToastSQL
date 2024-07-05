package server.databaseElements;

import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="Database")
public class Database {

	@XmlElementWrapper(name = "Tables")
	@XmlElement(name = "Table")
	private List<Table> tables;
	private @XmlAttribute String databaseName;
	
	public Database() {
		this.databaseName = "";
		this.tables = null;
	}
	
	public Database(String name) {
		this.databaseName = name;
		this.tables = null;
	}
	
	public Database(String name, List<Table> tables) {
		this.databaseName = name;
		this.tables = new ArrayList<Table>();
		tables.forEach(d -> {this.tables.add(d);});
	}
	
	public String getDbName() {
		return databaseName;
	}
	
	public List<Table> getTables() {
		return tables;
	}
	
	public Table getTable(String tableName) {
		if (tables == null) {
			return null;
		}
		for (Table t : tables) {
			if (t.getTableName().equals(tableName)) {
				return t;
			}
		}
		return null;
	}
	
	public int addTable(Table t) {
		if (tables == null) {
			tables = new ArrayList<Table>();
		}
		for (Table dbTable : tables) {
			if (dbTable.getTableName().equals(t.getTableName())) {
				return 1;
			}
		}
		tables.add(t);
		return 0;
	}
	
	public int removeTable(Table t) {
		
		if (tables != null && tables.contains(t)) {
			if (t.isTableReferenced(this)) {
				return 1;
			}
			tables.remove(t);
			if (tables.isEmpty()) {
				tables = null;
			}
			return 0;
		}
		return 1;
		
	}
	
}
