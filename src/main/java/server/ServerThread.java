package server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import jakarta.xml.bind.JAXBException;
import server.databaseElements.Attribute;
import server.databaseElements.Database;
import server.databaseElements.PKAttribute;
import server.databaseElements.Storage;
import server.databaseElements.Table;
import server.validator.InputValidator;
import server.validator.ValidatorDictionary;

public class ServerThread implements Runnable {
    private final Socket socket;
    private final PrintStream socketOutput;
    private final BufferedReader socketReader;
    private final InputValidator validator;
    private Server server;
    private ValidatorDictionary dict;
    
    public ServerThread(Socket socket, Server server, InputValidator validator) {
        try {
            this.socket = socket;
            this.socketOutput = new PrintStream(socket.getOutputStream());
            this.socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.server = server;
            this.validator = validator;
            dict = new ValidatorDictionary(server.getData());
            System.out.println("ServerThread started!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        try {
            // keres felepitese
            String command = readFullRequest();
            if (command.equals("GETALLDBS")) {
            	retrieveDbs();
            	socket.close();
                socketReader.close();
                socketOutput.close();
            	return;
            } else if (command.startsWith("GETFIELDSOF")) {
            	String[] commandParts = command.split(" ");
            	String db = commandParts[1];
            	String table = commandParts[2];
            	String[] fieldNames = getFieldsOfTable(db, table);
            	
            	if (fieldNames == null) {
                    socketOutput.print(",ERROR,");
                }
            	else {
	            	boolean first = true;
	            	for (String fieldName : fieldNames) {
	                    if (first) {
	                    	socketOutput.print(fieldName);
	                    	first = false;    
	                    } else {
	                        socketOutput.print("," + fieldName);
	                    }
	                }
                }
                
                socket.close();
                socketReader.close();
                socketOutput.close();
            	return;
            } else if (command.startsWith("GETPKSOF")) {
            	String[] commandParts = command.split(" ");
            	String db = commandParts[1];
            	String table = commandParts[2];
            	String[] fieldNames = getPKsOfTable(db, table);
            	
            	if (fieldNames == null) {
                    socketOutput.print(",ERROR,");
                }
            	else {
	            	boolean first = true;
	            	for (String fieldName : fieldNames) {
	                    if (first) {
	                    	socketOutput.print(fieldName);
	                    	first = false;    
	                    } else {
	                        socketOutput.print("," + fieldName);
	                    }
	                }
                }
                
                socket.close();
                socketReader.close();
                socketOutput.close();
            	return;
            }
            // debug
            // System.out.println("Received command:\n" + command);

            if (!validator.validate(command)) {
                socketOutput.println("Invalid syntax!");
            }
            else {
                ArrayList<String> commandLines = validator.getCommandLines();
                // valasz kuldese
                String answer = interpret(commandLines);
                socketOutput.println(answer);
            }


            // bezaras
            socket.close();
            socketReader.close();
            socketOutput.close();

        } catch (IOException e) {
            System.out.println("Server error occurred!");
            throw new RuntimeException(e);
        }
    }

    private String readFullRequest() throws IOException {
        String commandLine;
        StringBuilder command = new StringBuilder();
        int sizeOfMsg = Integer.parseInt(socketReader.readLine());
        while (sizeOfMsg > 0) {
            commandLine = socketReader.readLine();
            sizeOfMsg = sizeOfMsg - commandLine.length() - 1; // -1 for '\n'
            if (!command.isEmpty()) {
                command.append(" ");
            }
            command.append(commandLine);
        }

        return command.toString();
    }

    private String interpret(ArrayList<String> request) {
    	
    	String uri = "mongodb+srv://testUser:testPassword@toastcluster.az91s1m.mongodb.net/";
		MongoClient mongoClient = MongoClients.create(uri);
		ClientSession session = mongoClient.startSession();
		Storage data = server.getData();
		String answer = ValidatorDictionary.ok;
    	try {
    		session.startTransaction();
    		Storage backup = Server.XmlToStorage("toastsql.xml");
    		
    		for (String ln : request) {
    			answer = dict.executeRow(mongoClient, session, ln);
    			if (answer.split("\n")[0].matches("^\\d+$")) {
                    // select result
                	socketOutput.print(answer);
                } else if (!answer.equals(ValidatorDictionary.ok)) {
                    // error message
                    
    				// data = backup;
    				server.setData(backup);
    				session.abortTransaction();
    				return answer;
    			}
    		}
			data.convertObjectToXML("toastsql.xml");
		} catch (JAXBException e) {
			session.abortTransaction();
			mongoClient.close();
			return "Couldn't execute commands!";
		}
    	session.commitTransaction();
    	server.setData(data);
    	mongoClient.close();
    	return ValidatorDictionary.ok;
    }
    
    private void retrieveDbs() {
    	StringBuilder sb = new StringBuilder();
    	for (Database db : server.getData().getDatabases()) {
    		sb.append(db.getDbName());
    		sb.append(" ");
    		List<Table> dbTables = db.getTables();
    		if (dbTables != null) {
	    		for (Table t : dbTables) {
	    			sb.append(t.getTableName());
	    			sb.append(" ");
	    		}
    		}
    		else {
    			sb.append(" ");
    		}
    		sb.append(" ");
    	}
    	socketOutput.println(sb.toString());
    }
    
    private String[] getFieldsOfTable(String dbName, String tableName) {
        try {
	        List<Attribute> attrs = server.getData().getDatabase(dbName).getTable(tableName).getAttributes();
	        String[] attrNames = new String[attrs.size()];
	        int i = 0;
	        for (Attribute attr : attrs) {
	            attrNames[i] = attr.getAttributeName();
	            i++;
	        }
	        return attrNames;
        } catch (NullPointerException e) {
            return null;
        }
    }
    
    private String[] getPKsOfTable(String dbName, String tableName) {
        try {
	        List<PKAttribute> attrs = server.getData().getDatabase(dbName).getTable(tableName).getPrimaryKeys();
	        String[] attrNames = new String[attrs.size()];
	        int i = 0;
	        for (PKAttribute attr : attrs) {
	            attrNames[i] = attr.getPKAttributeName();
	            i++;
	        }
	        return attrNames;
        } catch (NullPointerException e) {
            return null;
        }
    }
}