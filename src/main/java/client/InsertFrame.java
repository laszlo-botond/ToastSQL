package client;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTable;
import javax.swing.SwingConstants;

import server.validator.ValidatorDictionary;

public class InsertFrame extends JFrame {
    private JTable table;
    private JPanel fieldNamePanel;
    private List<String> databases;
    private JComboBox<String> cbDb;
    private JComboBox<String> cbTable;
    private JPanel cbPanel;
    private JPanel contentPanel;
    private Map<String, ArrayList<String>> dbTables;
    private String[] activeFields;
    private List<Color> colors;
    private JLabel answerLabel;
    
    public InsertFrame() {
    	this.setTitle("ToastSQL Insert GUI");
    	this.setIconImage(ClientFrame.getIcon());
    	colors = ClientFrame.getColors();
    	
    	// container panels
        contentPanel = new JPanel(new BorderLayout());
        contentPanel.setBackground(colors.get(0));
        cbPanel = new JPanel(new GridLayout(1,2));
        cbPanel.setBackground(colors.get(0));
        // contentPanel elements
        table = new JTable();
        table.setBackground(colors.get(0));
        table.setForeground(colors.get(2));
        fieldNamePanel = new JPanel();
        fieldNamePanel.setBackground(colors.get(1));
        // cbPanel elements
        cbDb = new JComboBox<>();
        cbDb.setBackground(colors.get(1));
        cbDb.setForeground(colors.get(2));
        cbTable = new JComboBox<>();
        cbTable.setBackground(colors.get(1));
        cbTable.setForeground(colors.get(2));

        answerLabel = new JLabel("");
        
        contentPanel.add(table);
        contentPanel.add(fieldNamePanel, BorderLayout.NORTH);
        add(contentPanel);

        cbPanel.add(cbDb);
        cbPanel.add(cbTable);

        createComboBox();

        JPanel buttonPanel = new JPanel(new GridLayout(2, 1));
        buttonPanel.setBackground(colors.get(0));
        JButton button = new JButton("INSERT");
        button.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(ActionEvent e) {
                createAndSendRequest();
            }
        });
        button.setBounds(0,200,200,200);

        buttonPanel.add(answerLabel);
        buttonPanel.add(button);
        add(buttonPanel, BorderLayout.SOUTH);

        setBounds(100, 100, 500, 500);
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);
        setVisible(true);
    }

    public void createComboBox() {
        loadDatabases();

        if (databases == null || databases.isEmpty()) {
            JPanel errPanel = new JPanel();
            errPanel.setBackground(colors.get(0));
            JLabel errLabel = new JLabel("No databases exist!");
            errLabel.setForeground(colors.get(2));
            errPanel.add(errLabel);
            add(errPanel);
            return;
        }

        // convert databases to String[]
        String[] stringDb = new String[databases.size()];
        int i = 0;
        for (String s : databases) {
            stringDb[i] = s;
            i++;
        }

        // create ComboBox with databases as choices
        cbDb = new JComboBox<String>(stringDb);
        cbDb.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                createTableChoices(cbDb.getSelectedItem().toString());
                createTable();
            }
        });

        createTableChoices(stringDb[0]);
        cbPanel.removeAll();
        cbPanel.add(cbDb);
        cbPanel.add(cbTable);
        add(cbPanel, BorderLayout.NORTH);

        revalidate();
        repaint();
    }

    public void createTableChoices(String db) {
        cbPanel.remove(cbTable);
        ArrayList<String> ownTables = dbTables.get(db);
        if (ownTables == null || ownTables.isEmpty()) {
            cbPanel.remove(cbTable);
            cbPanel.revalidate();
            return;
        }
        // convert databases to String[]
        String[] stringTables = new String[ownTables.size()];
        int i = 0;
        for (String s : ownTables) {
            stringTables[i] = s;
            i++;
        }

        // create ComboBox with databases as choices
        cbTable = new JComboBox<String>(stringTables);
        cbTable.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                createTable();
            }
        });
        cbPanel.add(cbTable);
        cbPanel.revalidate();

        revalidate();
    }

    public void createTable() {
        // get fields
        activeFields = getFieldsOfTable(cbDb.getSelectedItem().toString(), cbTable.getSelectedItem().toString());
        
        contentPanel.removeAll();
        remove(contentPanel);

		if (activeFields == null) {
            table = new JTable(1, 1);
            contentPanel.revalidate();
        	contentPanel.repaint();
        	add(contentPanel);
        	revalidate();
        	repaint();
            return;
        }

        int fieldCount = activeFields.length;
        fieldNamePanel = new JPanel(new GridLayout(1, fieldCount));
        fieldNamePanel.setBackground(colors.get(1));
        JLabel fieldNameLabel;
        for (String field : activeFields) {
            fieldNameLabel = new JLabel(field, SwingConstants.CENTER);
            fieldNameLabel.setForeground(colors.get(2));
            fieldNamePanel.add(fieldNameLabel);
        }
        contentPanel.add(fieldNamePanel, BorderLayout.NORTH);
        table = new JTable(20, fieldCount);
        table.setBackground(colors.get(0));
        table.setForeground(colors.get(2));
        contentPanel.add(table);

        add(contentPanel);
        contentPanel.revalidate();
        contentPanel.repaint();
    }

    public void loadDatabases() {
        try {
            Socket socket = new Socket("localhost", 12000);
            PrintStream socketOutput = new PrintStream(socket.getOutputStream());
            socketOutput.println("GETALLDBS".length());
            socketOutput.println("GETALLDBS");

            BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String answerReceived = socketReader.readLine();
            String[] answerSplit = answerReceived.split(" ");

            databases = new ArrayList<>();
            dbTables = new HashMap<>();
            ArrayList<String> currentTables = new ArrayList<>();
            String lastDb = "";

            boolean nextIsDB = true;

            for (String s : answerSplit) {
//                System.out.println("Line: " + s);
                if (!nextIsDB && !s.isEmpty()) {
                    currentTables.add(s);
                }

                if (nextIsDB) {
                    databases.add(s);
                    nextIsDB = false;

                    // remember this as db that next tables belong to
                    lastDb = s;
                } else if (s.trim().isEmpty()) {
                    nextIsDB = true;

                    // confirm this db's tables in map
                    dbTables.put(lastDb, currentTables);

                    // reset table list
                    currentTables = new ArrayList<>();
                }
            }
            // confirm last db's tables in map
            dbTables.put(lastDb, currentTables);
            socket.close();
        } catch (IOException e) {
            System.out.println("Databases error");
        }

    }

	private String[] getFieldsOfTable(String dbName, String tableName) {
        try {
        	Socket socket = new Socket("localhost", 12000);
        	PrintStream socketOutput = new PrintStream(socket.getOutputStream());
        	
        	// send message
        	String msg = "GETFIELDSOF " + dbName + " " + tableName;
        	msg = msg.length() + "\n" + msg;
        	socketOutput.println(msg);
        	
        	// receive answer
        	BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String answerReceived = socketReader.readLine();
            if (answerReceived.equals(",ERROR,")) {
            	socket.close();
            	return null;
            }
            String[] fields = answerReceived.split(",");
            socket.close();
            return fields;
        } catch (IOException e) {
            System.out.println("Error");
        }
        return null;
    }

	private void createAndSendRequest() {
        if (activeFields == null) {
            // table doesn't exist
            return;
        }
        
        int tableRows = table.getRowCount();
        int tableCols = table.getColumnCount();
        boolean[] given = new boolean[tableCols];
        
        String fullCommand = "USE " + cbDb.getSelectedItem() + " \n";
        String commandLine;
        
        for (int i=0; i<tableRows; i++) {
            commandLine = "";
            
            // set all fields to not given
            for (int j=0; j<tableCols; j++) {
            	given[j] = false;
            }
            
            
            boolean wasAnyContent = false;
            // check which fields were filled in this row
            for (int j=0; j<tableCols; j++) {
                if (table.getValueAt(i,j) != null && !table.getValueAt(i, j).equals("")) {
                    // was not left empty
                    given[j] = true;
                    wasAnyContent = true;
                }
            }
            if (!wasAnyContent) {
                break;
            }
            
            // build start of string
            commandLine = "INSERT INTO " + cbTable.getSelectedItem() + "(";
            for (int j=0; j<tableCols; j++) {
                if (given[j]) {
                    commandLine = commandLine + activeFields[j] + ",";
                }
            }
            
			// fix last comma and close bracket
			commandLine = commandLine.substring(0, commandLine.length()-1) + ") VALUES (";
            
            // add actual field values
            for (int j=0; j<tableCols; j++) {
                if (given[j]) {
                	commandLine = commandLine + table.getValueAt(i, j) + ",";
                }
            }
            
            // fix last comma and close bracket
            commandLine = commandLine.substring(0, commandLine.length()-1) + ")\n";
            
            fullCommand = fullCommand + commandLine;
        }
        System.out.println("Insertframe submit: " + fullCommand);
        
        try {
        	Socket socket = new Socket("localhost", 12000);
        	// send request
        	ClientFrame.sendRequest(socket, fullCommand);
        	// receive answer
        	BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        	String answerReceived = socketReader.readLine();
        	setAnsLabel(answerReceived);

        	socket.close();
        } catch (IOException e) {
            setAnsLabel("Error: Couldn't connect to server!");
        }
    }
	
	private void setAnsLabel(String ans) {
		answerLabel.setText(ans);
		if (ans.equals(ValidatorDictionary.ok)) {
			answerLabel.setForeground(Color.GREEN);
		} else {
			answerLabel.setForeground(Color.RED);
		}
		answerLabel.revalidate();
		contentPanel.repaint();
	}

    public static void main(String[] args) {
        new InsertFrame();
    }
}