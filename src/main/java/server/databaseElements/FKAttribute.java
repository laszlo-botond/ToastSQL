package server.databaseElements;

import jakarta.xml.bind.annotation.*;

@XmlRootElement(name="FKAttribute")
public class FKAttribute {
	private @XmlElement(name="Referee") String attr;
	private @XmlElement(name="References") References ref;
	
	public FKAttribute() {}
	
	public FKAttribute(String attr, String refTable, String refAttr) {
		this.attr = attr;
		this.ref = new References(refTable, refAttr);
	}
	
	public References getReference() {
		return ref;
	}
	
	public String getFKAttributeName() {
		return attr;
	}
}
