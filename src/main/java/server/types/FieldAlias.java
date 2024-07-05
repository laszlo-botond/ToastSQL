package server.types;

public class FieldAlias {
	private String tableName;
	private boolean isPK;
	private String attrName;
	private int pos;
	
	public FieldAlias(String tableName, boolean isPK, String attrName, int pos) {
		
		this.tableName = tableName;
		this.isPK = isPK;
		this.attrName = attrName;
		this.pos = pos;
	}

	public String getTableName() {
		return tableName;
	}

	public boolean isPK() {
		return isPK;
	}

	public String getAttrName() {
		return attrName;
	}

	public int getPos() {
		return pos;
	}
	
	
	
}
