package server;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Main2 {
	public static void main(String[] args) throws IOException {
		System.out.println(Double.parseDouble("2.2"));
		
//		System.out.println(String.format("%011d", -1234567891));
//		
//		for (int i = 0; i < 1001; i++) {
//			Socket socket = new Socket("localhost", 12000);
//			BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//			String msg = generateData("SzaTabla", i * 100, (i + 1) * 100);
//			System.out.println(msg);
//			ClientFrame.sendRequest(socket, msg);
//			
//			String ans = socketReader.readLine();
//			System.out.println("[" + i + "]: " + ans);
//			socket.close();
//		}
		
	}

	
	private static String generateData(String tableName, int low, int count) {
		StringBuilder command = new StringBuilder("USE SzazEzer INSERT INTO " + tableName + "(ID, Name, Average) VALUES ");
		int length = 1;
		for (int i=low; i<count; i++) {
			command.append("(" + i + ",");
			
			command.append("\"");
			for (int j=0; j<length; j++) {
				char c = (char) ((char) 'a' + i%26);
				command.append(c);
			}
			command.append("\",");
			if (i % 471 == 0) {
				length = Math.min(length+1, 64);	
			}
			
			command.append(Math.random());
			
			command.append("),\n");
		}
		command.deleteCharAt(command.length()-1);
		command.deleteCharAt(command.length()-1);
		return command.toString();
	}
}
