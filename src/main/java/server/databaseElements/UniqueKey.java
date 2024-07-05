package server.databaseElements;

import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="UniqueKey")
public class UniqueKey{
	
	@XmlElement(name="UQAttribute")
	private List<UQAttribute> uqAttr;
	
	@XmlAttribute
	private String uqName;
	
	public UniqueKey() {

	}
	
	public UniqueKey(String uqName) {
		this.uqName = uqName;
		this.uqAttr = new ArrayList<UQAttribute>();
	}
	
	public String getName() {
		return this.uqName;
	}
	
	public int addToUQ(Table t, String attrName) {
		
		Attribute a = t.getAttribute(attrName);
		if (a == null) {
			return 1;
		}
		for (UQAttribute attr : uqAttr) {
			if (attr.getName().equals(attrName)) {
				return 1;
			}
		}
		
		uqAttr.add(new UQAttribute(attrName));
		return 0;
	}
	
	public List<UQAttribute> getUqAttr() {
		return uqAttr;
	}
}
