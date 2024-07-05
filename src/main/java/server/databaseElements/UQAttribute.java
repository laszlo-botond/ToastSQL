package server.databaseElements;

import jakarta.xml.bind.annotation.*;

@XmlRootElement(name="UQAttribute")
public class UQAttribute {
	private @XmlValue String field;
	public UQAttribute() {
		
	}
	
	public UQAttribute(String field) {
		this.field = field;
	}
	
	public String getUQAttributeName() {
		return field;
	}
	
	public String getName() {
		return field;
	}
}
