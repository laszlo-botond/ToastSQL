package server.databaseElements;

import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Table")
public class Table {

	private @XmlAttribute String tableName;

	@XmlElementWrapper(name = "Structure")
	@XmlElement(name = "Attribute")
	private List<Attribute> attr;

//	@XmlElementWrapper(name="PrimaryKey")
//	@XmlElement(name="PKAttribute")
//	private List<PKAttribute> pk;

	@XmlElement(name = "PrimaryKey")
	private PrimaryKey pk;

	@XmlElementWrapper(name = "ForeignKeys")
	@XmlElement(name = "ForeignKey")
	private List<ForeignKey> fk;

	@XmlElementWrapper(name = "UniqueKeys")
	@XmlElement(name = "UniqueKey")
	private List<UniqueKey> uk;

	@XmlElementWrapper(name = "IndexFiles")
	@XmlElement(name = "IndexFile")
	private List<Index> indf;

	// KONSTRUKTOROK
	public Table() {

	}

	public Table(String name) {
		this.tableName = name;
	}

	public Table(String name, List<Attribute> attr, PrimaryKey pk, List<ForeignKey> fk, List<UniqueKey> uk,
			List<Index> indf) {
		this.tableName = name;
		if (attr != null) {
			this.attr = new ArrayList<Attribute>();
			attr.forEach(d -> {
				this.attr.add(d);
			});
		}
		if (pk != null) {
			this.pk = new PrimaryKey(pk.getName());
			pk.getPkAttr().forEach(d -> {
				this.pk.getPkAttr().add(d);
			});
		}
		if (fk != null) {
			this.fk = new ArrayList<ForeignKey>();
			fk.forEach(d -> {
				this.fk.add(d);
			});
		}
		if (uk != null) {
			this.uk = new ArrayList<UniqueKey>();
			uk.forEach(d -> {
				this.uk.add(d);
			});
		}
		if (indf != null) {
			this.indf = new ArrayList<Index>();
			indf.forEach(d -> {
				this.indf.add(d);
			});
		}
	}

	// GETTEREK
	public String getTableName() {
		return this.tableName;
	}

	public Attribute getAttribute(String attrName) {
		if (attr == null) {
			return null;
		}

		for (Attribute a : attr) {
			if (a.getAttributeName().equals(attrName)) {
				return a;
			}
		}
		return null;
	}

	public List<Index> getIndexFiles() {
		return indf;
	}
	
	public Index getIndexFile(String name) {
		if (indf == null) {
			return null;
		}

		for (Index i : indf) {
			if (i.getIndexName().equals(name)) {
				return i;
			}
		}
		return null;
	}

	public List<PKAttribute> getPrimaryKeys() {
		if (pk == null)
			return null;
		return pk.getPkAttr();
	}

	public List<ForeignKey> getForeignKeys() {
		return fk;
	}

	public ForeignKey getForeignKey(String name) {
		if (fk == null) {
			return null;
		}

		for (ForeignKey f : fk) {
			if (f.getFKName().equals(name)) {
				return f;
			}
		}
		return null;
	}

	public List<Attribute> getAttributes() {
		return attr;
	}

	public List<UniqueKey> getUniqueKeys() {
		return uk;
	}

	public Index getIndexOnAttribute(Attribute attr) {
		if (getIndexFiles() == null) {
			return null;
		}
		for (Index ind : getIndexFiles()) {
			if (ind.getIndexes().size() == 1 && ind.getIndexes().get(0).getAttr().equals(attr.getAttributeName())) {
				return ind;
			}
		}
		return null;
	}
	
	// ATTRIBUTE LOGIC
	public int addAttribute(String name, String type, String length, String isnull, String defaultVal) {
		if (getAttribute(name) != null) {
			return 1;
		}

		if (attr == null) {
			attr = new ArrayList<Attribute>();
		}

		attr.add(new Attribute(name, type, length, isnull, defaultVal));
		return 0;
	}

	public int removeAttribute(String attrName) {
		// TODO: check if PK or FK
		Attribute a = getAttribute(attrName);
		if (a == null) {
			return 1;
		}

		attr.remove(a);
		if (attr.isEmpty()) {
			attr = null;
		}
		return 0;
	}

	// PRIMARY KEY LOGIC
	public boolean isTableReferenced(Database db) {
		if (db == null || db.getTables() == null) {
			return false;
		}
		for (Table t : db.getTables()) {
			List<ForeignKey> fks = t.getForeignKeys();
			if (fks == null) {
				continue;
			}
			for (ForeignKey fk : fks) {
				if (fk.getReferencedTable().equals(this.getTableName())) {
					return true;
				}
			}
		}
		return false;
	}

	public List<Table> getReferencingTables(Database db) {
		List<Table> retList = new ArrayList<>();
		if (db == null || db.getTables() == null) {
			return retList;
		}
		for (Table t : db.getTables()) {
			List<ForeignKey> fks = t.getForeignKeys();
			if (fks == null) {
				continue;
			}
			for (ForeignKey fk : fks) {
				if (fk.getReferencedTable().equals(this.getTableName())) {
					retList.add(t);
				}
			}
		}
		return retList;
	}
	
	public int addPK(String pkName) {
		if (pk != null || hasConstraintNamed(pkName)) {
			return 1;
		}

		this.pk = new PrimaryKey(pkName);
		return 0;
	}

	public int addToPK(String attrName) {
		if (pk == null) {
			pk = new PrimaryKey("PK_" + this.getTableName());
		}
		return pk.addToPK(this, attrName);
	}

	public int removeFromPK(String attrName, Database db) {
		if (pk == null) {
			return 1;
		}
		int retVal = pk.removeFromPK(this, attrName);
		if (retVal == 1) {
			return 1;
		}

		if (pk.getPkAttr().isEmpty()) {
			pk = null;
		}
		return 0;

	}

	public int removePK() {
		if (this.pk == null) {
			return 1;
		}
		this.pk = null;
		return 0;
	}

	// FOREIGN KEY LOGIC
	public int addFK(String fkName) {
		if (hasConstraintNamed(fkName)) {
			return 1;
		}
		if (fk == null) {
			fk = new ArrayList<ForeignKey>();
		}
		fk.add(new ForeignKey(fkName));
		return 0;
	}
	
	public int addFK(ForeignKey newFK) {
		if (hasConstraintNamed(newFK.getFKName())) {
			return 1;
		}
		if (fk == null) {
			fk = new ArrayList<ForeignKey>();
		}
		fk.add(newFK);
		return 0;
	}

	public int removeFK(String fkName) {
		if (fk == null || getForeignKey(fkName) == null) {
			return 1;
		}

		for (ForeignKey fkey : fk) {
			if (fkName.equals(fkey.getFKName())) {
				fk.remove(fkey);
				if (fk.isEmpty()) {
					fk = null;
				}
				return 0;
			}
		}

		return 1;
	}

	// UNIQUE LOGIC
	public String generateUQName() {
		if (uk == null) {
			return "UQ_" + this.getTableName() + "_1";
		}

		int nr = 0;
		String tmp = "";
		boolean hasUQ = true;
		while (hasUQ) {
			nr++;
			hasUQ = false;
			tmp = "UQ_" + this.getTableName() + "_" + nr;
			for (UniqueKey uq : uk) {
				if (uq.getName().equals(tmp)) {
					hasUQ = true;
				}
			}
		}
		return tmp;
	}

	public int addUQ(String uqName) {
		if (hasConstraintNamed(uqName)) {
			return 1;
		}
		if (uk == null) {
			uk = new ArrayList<UniqueKey>();
		}
		for (UniqueKey key : uk) {
			if (key.getName().equals(uqName)) {
				if (uk.isEmpty()) {
					uk = null;
				}
				return 1;
			}
		}

		uk.add(new UniqueKey(uqName));
		return 0;
	}

	public int addToUQ(String uqName, String attrName) {
		if (uk == null) {
			return 1;
		}

		for (UniqueKey uq : uk) {
			if (uq.getName().equals(uqName)) {
				return uq.addToUQ(this, attrName);
			}
		}
		return 1;
	}

	/*
	 * public int removeFromUQ(String attrName) { Attribute a =
	 * getAttribute(attrName); if (a == null || !a.getUQ() || uk == null) { return
	 * 1; }
	 * 
	 * a.setUQ(false); for (UQAttribute ukey : uk) { if
	 * (a.getAttributeName().equals(ukey.getUQAttributeName())) { uk.remove(ukey);
	 * if (uk.isEmpty()) { uk = null; } return 0; } } return 1; }
	 */

	public int removeUQ(String uqName) {
		if (uk == null) {
			return 1;
		}
		for (UniqueKey uq : uk) {
			if (uq.getName().equals(uqName)) {
				uk.remove(uq);
				return 0;
			}
		}
		return 1;
	}

	// CONSTRAINT CHECK
	public boolean hasConstraintNamed(String name) {
		if (pk != null && pk.getName().equals(name)) {
			return true;
		}
		if (uk != null) {
			for (UniqueKey uq : uk) {
				if (uq.getName().equals(name)) {
					return true;
				}
			}
		}
		if (fk != null) {
			for (ForeignKey fq : fk) {
				if (fq.getFKName().equals(name)) {
					return true;
				}
			}
		}
		return false;
	}

	// INDEX FILE LOGIC
	public int addIndexFile(String fileName, String isUnique) {
		if (this.getPrimaryKeys() == null || this.getPrimaryKeys().size() == 0) {
			return 1;
		}
		
		if (getIndexFile(fileName) != null) {
			return 1;
		}
		if (indf == null) {
			indf = new ArrayList<Index>();
		}

		indf.add(new Index(fileName, isUnique));
		return 0;
	}

	public int addIndexFile(Index indFile) {
		if (this.getPrimaryKeys() == null || this.getPrimaryKeys().size() == 0) {
			return 1;
		}
		if (getIndexFile(indFile.getIndexName()) != null) {
			return 1;
		}
		if (indf == null) {
			indf = new ArrayList<Index>();
		}

		indf.add(indFile);
		return 0;
	}
	
	public List<Integer> getAttrPos(String attrName) {
		int posPK = 0;
		int posVal = 0;
		List<Integer> retList = new ArrayList<Integer>();
		for (Attribute attr : getAttributes()) {
			if (attr.getAttributeName().equals(attrName)) {				
				if (attr.isPK(this)) {
					retList.add(1);
					retList.add(posPK);
					return retList;
				}
				else {
					retList.add(0);
					retList.add(posVal);	
					return retList;
				}
			}
			if (attr.isPK(this)) {
				posPK++;
			}
			else {
				posVal++;
			}
		}
		retList.add(-1);
		retList.add(-1);
		return retList;
	}
	
	public int removeIndexFile(String fileName) {
		Index i = getIndexFile(fileName);
		if (i == null) {
			return 1;
		}
		indf.remove(i);
		if (indf.isEmpty()) {
			indf = null;
		}
		return 0;
	}
}
