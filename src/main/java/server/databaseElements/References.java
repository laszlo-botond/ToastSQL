package server.databaseElements;

import jakarta.xml.bind.annotation.*;

@XmlRootElement(name="References")
public class References {
	
	private @XmlElement String refTable;
	private @XmlElement String refAttr;
	
	public References() {
		
	}
	
	public References(String table, String attr) {
		refTable = table;
		refAttr = attr;
	}
	
	public String getRefAttr() {
		return refAttr;
	}
	
	public String getRefTable() {
		return refTable;
	}
	
}
