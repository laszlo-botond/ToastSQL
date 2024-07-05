package server.databaseElements;

import jakarta.xml.bind.annotation.*;

@XmlRootElement(name="IAttribute")
public class IndexAttribute {
	
	private @XmlValue String attr;

	public IndexAttribute() {}

	public IndexAttribute(String attr) {
		this.attr = attr;
	}
	
	public String getAttr() {
		return attr;
	}
}
