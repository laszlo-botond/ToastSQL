package server;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import server.databaseElements.Storage;
import server.validator.InputValidator;

public class Server {
    
	private Storage data;
	private final InputValidator validator;
	
	public Server() {
		validator = new InputValidator();
        try {
        	File f = new File("toastsql.xml");
        	if (f.exists() && !f.isDirectory()) {
        		data = XmlToStorage("toastsql.xml");
        	}
        	else {
        		// System.out.println("File Created!");
        		data = new Storage();
        		data.convertObjectToXML("toastsql.xml");
        	}
        	
            Socket socket;
            ServerSocket szerverSocket = new ServerSocket(12000);
            System.out.println("Server started!");

            
            
            while (true) {
                // socket a kommunikaciohoz
                socket = szerverSocket.accept();
                ServerThread serverThread = new ServerThread(socket, this, validator);

                // thread letrehozasa es futtatasa
                Thread answerThread = new Thread(serverThread);
                answerThread.start();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (JAXBException e) {
        	try {
        		data = new Storage();
				data.convertObjectToXML("toastsql.xml");
				new Server();
			} catch (JAXBException e1) {
				System.out.println("Marshal operation failed!");
				return;
			}
        }
    }

    public static void main(String[] args) {
        new Server();
    }
    
    public static Storage XmlToStorage(String fileName) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance(Storage.class);
		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		Storage data = (Storage) jaxbUnmarshaller.unmarshal(new File(fileName));
		return data;
	}
	
	public Storage getData() {
		return data;
	}
	
	public void setData(Storage data) {
		this.data = data;
	}
    
    
}
