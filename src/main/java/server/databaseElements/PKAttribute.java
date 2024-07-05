package server.databaseElements;

import jakarta.xml.bind.annotation.*;


@XmlRootElement(name="PKAttribute")
public class PKAttribute {
	
	private @XmlValue String field;
	
	public PKAttribute() {
	}
	
	public PKAttribute(String field) {
		this.field = field;
	}
	
	public String getPKAttributeName() {
		return field;
	}
	
}
