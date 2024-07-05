package server.databaseElements;

import java.util.List;

import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="ForeignKey")
public class ForeignKey {

	@XmlAttribute
	private String fkName;
	
//	@XmlElementWrapper(name="FKAttributes")
	@XmlElement(name="FKAttribute")
	private List<FKAttribute> fkList;
	
	public ForeignKey() {
		
	}
	
	public ForeignKey(String fkName) {
		this.fkName = fkName;
	}
	
	public String getReferencedTable() {
		if (fkList != null && fkList.size() > 0) {
			return fkList.get(0).getReference().getRefTable();
		}
		return null;
	}
	
	public String getFKName() {
		return fkName;
	}
	
	public List<FKAttribute> getFKAttributes() {
		return fkList;
	}
	
	public void setFkList(List<FKAttribute> fkList) {
		this.fkList = fkList;
	}
	
}
