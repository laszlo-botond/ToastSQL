package server.databaseElements;

import java.util.List;

import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlValue;

@XmlRootElement(name="Attribute")
public class Attribute {

	private @XmlValue String attributeName;
	private @XmlAttribute String type;
	private @XmlAttribute String length;
	private @XmlAttribute String isnull;
	private @XmlAttribute String defaultVal;
	
	public Attribute() {
		
	}
	
	public Attribute(String name, String type, String length, String isnull, String defaultVal) {
		this.attributeName = name;
		this.type = type;
		this.length = length;
		this.isnull = isnull;
		this.defaultVal = defaultVal;
	}
	
	public String getAttributeName() {
		return this.attributeName;
	}
	
	public String getAttributeType() {
		return this.type;
	}
	
	public String getAttributeLength() {
		return length;
	}
	
	public boolean isNullable() {
		if (isnull == null || isnull.equals("1")) {
			return true;
		}
		return false;
	}
	
	public boolean isPK(Table t) {
		List<PKAttribute> pkList = t.getPrimaryKeys();
		if (pkList == null) {
			return false;
		}
		
		for (PKAttribute pk : pkList) {
			if (pk.getPKAttributeName().equals(this.attributeName)) {
				return true;
			}
		}
		
		return false;
	}
	
	public String getDefault() {
		return defaultVal;
	}
	
	public int getSize() {
		switch(type.toUpperCase()) {
		case "INT": return 4;
		case "DATE": return "2020-01-01".getBytes().length;
		case "DATETIME": return "2020-01-01 00:00:00".getBytes().length;
		case "DOUBLE": return 8;
		case "FLOAT": return 4;
		case "VARCHAR": return Integer.parseInt(length);
		case "CHAR": return Integer.parseInt(length);
		
		default: return 0;
		}
	}
}
