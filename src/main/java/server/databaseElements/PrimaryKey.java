package server.databaseElements;

import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="PrimaryKey")
public class PrimaryKey{

	@XmlElement(name="PKAttribute")
	private List<PKAttribute> pkAttr;
	
	private @XmlAttribute String pkName;
	
	public PrimaryKey() {
	}
	
	public PrimaryKey(String pkName) {
		this.pkName = pkName;
		this.pkAttr = new ArrayList<PKAttribute>();
	}
	
	public String getName() {
		return this.pkName;
	}
	
	public int addToPK(Table t, String attrName) {
		Attribute a = t.getAttribute(attrName);
		if (a == null) {
			return 1;
		}
		for (PKAttribute attr : pkAttr) {
			if (attr.getPKAttributeName().equals(attrName)) {
				return 1;
			}
		}
		
		pkAttr.add(new PKAttribute(a.getAttributeName()));
		return 0;
	}

	public int removeFromPK(Table t, String attrName) {
		Attribute a = t.getAttribute(attrName);
		if (a == null) {
			return 1;
		}
		for (PKAttribute attr : pkAttr) {
			if (attr.getPKAttributeName().equals(attrName)) {
				pkAttr.remove(attr);
				return 0;
			}
			
		}
		return 1;
	}
	
	public List<PKAttribute> getPkAttr() {
		return pkAttr;
	}
}
