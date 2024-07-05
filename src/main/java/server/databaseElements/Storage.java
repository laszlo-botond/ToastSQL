package server.databaseElements;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.annotation.*;

@XmlRootElement(name="Databases")
public class Storage {
	
	
	@XmlElement(name="Database")
	private List<Database> dbs;
	
	public Storage() {}
	
	public List<Database> getDatabases() {
		return dbs;
	}
	
	public Database getDatabase(String dbName) {
		if (dbs == null) {
			return null;
		}
		
		for (Database db : dbs) {
			if (db.getDbName().equals(dbName)) {
				return db;
			}
		}
		return null;
	}
	
	public int addDatabase(Database db) {
		if (dbs == null) {
			dbs = new ArrayList<Database>();
		}
		if (this.getDatabase(db.getDbName()) != null) {
			return 1;
		}
		dbs.add(db);
		
		return 0;
	}
	
	public int removeDatabase(Database db) {
		if (dbs == null || !dbs.contains(db)) {
			return 1;
		}
		
		dbs.remove(db);
		if (dbs.isEmpty()) {
			dbs = null;
		}
		return 0;
	}
	
	public void convertObjectToXML(String fileName) throws JAXBException {
		FileWriter fw;
		BufferedWriter out;
		try {
			fw = new FileWriter(fileName, false);
			out = new BufferedWriter(fw);
		} catch(IOException e) {
			System.out.println("File Not Found!");
			return;
		}
		
		JAXBContext context = JAXBContext.newInstance(this.getClass());
		Marshaller m = context.createMarshaller();
		m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
		//m.marshal(obj, System.out);
		m.marshal(this, out);
		
		try {
			out.close();
		} catch (IOException e) {
			System.out.println("Error closing file!");
		}
	}
	
}
