package server.databaseElements;

import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.*;

@XmlRootElement(name="Index")
public class Index {
	
	@XmlAttribute
	private String name;
	
	@XmlAttribute
	private String isUnique;
	
	
	// @XmlElementWrapper(name="IndexAttributes")
	@XmlElement(name="IAttribute")
	private List<IndexAttribute> indexList;
	
	public Index() {}
	
	public Index(String fileName, String isUnique) {
		this.name = fileName;
		this.isUnique = isUnique;
	}
	
	public Index(String fileName, String isUnique, List<IndexAttribute> il) {
		this.name = fileName;
		this.isUnique = isUnique;
		if (il != null) {
			this.indexList = new ArrayList<IndexAttribute>();
			il.forEach(d -> {this.indexList.add(d);});
		}
	}
	
	public String getIndexName() {
		return name;
	}
	
	public boolean isIndexUnique() {
		if (isUnique == null || !isUnique.equals("1")) {
			return false;
		}
		return true;
	}
	
	public List<IndexAttribute> getIndexes() {
		return indexList;
	}
	
	public void setIndexList(List<IndexAttribute> indList) {
		this.indexList = indList;
	}
	
	public IndexAttribute getIndex(String attr) {
		if (indexList == null) {
			return null;
		}
		
		for (IndexAttribute i : indexList) {
			if (i.getAttr().equals(attr)) {
				return i;
			}
		}
		return null;
	}
	
	public int addIndex(Table t, String attr) {
		Attribute a = t.getAttribute(attr);
		if (a == null || getIndex(attr) != null) {
			return 1;
		}
		if (indexList == null) {
			indexList = new ArrayList<IndexAttribute>();
		}
		
		indexList.add(new IndexAttribute(attr));
		return 0;
	}
	
	public int removeIndex(String attr) {
		if (indexList == null || getIndex(attr) == null) {
			return 1;
		}
		indexList.remove(getIndex(attr));
		if (indexList.isEmpty()) {
			indexList = null;
		}
		return 0;
	}
}
