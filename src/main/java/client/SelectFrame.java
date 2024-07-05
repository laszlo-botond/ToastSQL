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
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.border.EmptyBorder;
import javax.swing.plaf.basic.BasicScrollBarUI;

import server.validator.ValidatorDictionary;

public class SelectFrame extends JFrame {

	private List<String> databases;
	private Map<String, ArrayList<String>> dbTables;
	private JComboBox<String> cbDb;
	private JComboBox<String> cbTable;
	private List<Color> colors;
	
	private JPanel headerPanel;
	private JPanel resultPanel;
	private JPanel tablePanel;
	private JPanel joinPanel;
	private JTable joinTable;
	private JPanel wherePanel;
	private JTable whereTable;
	private JPanel selectPanel;
	private JTable selectTable;
	private JCheckBox selectCheckBox;
	private JButton executeButton;
	private JLabel answerLabel;
	
	public SelectFrame() {
		this.setDefaultCloseOperation(DISPOSE_ON_CLOSE);
		this.setBounds(100, 100, 1000, 600);
		this.setTitle("ToastSQL Query GUI");
		this.setIconImage(ClientFrame.getIcon());
		colors = ClientFrame.getColors();
		
		headerPanel = new JPanel();
		JLabel fromLabel = new JLabel("FROM: ");
		fromLabel.setForeground(colors.get(2));
		headerPanel.setBackground(colors.get(0));
		
		cbTable = new JComboBox<>();
		headerPanel.add(fromLabel);
		headerPanel.add(cbTable);
		
		
		JPanel centerPanel = new JPanel();
		centerPanel.setLayout(new GridLayout(2, 1));
		
		JPanel queryPanel = new JPanel();
		queryPanel.setLayout(new GridLayout(1, 3));
		
		initializeJoinPanel();
		initializeWherePanel();
		initializeSelectPanel();
		initializeOutputPanel();
		
		queryPanel.add(joinPanel);
		queryPanel.add(wherePanel);
		queryPanel.add(selectPanel);
		

		createComboBox();
		
		centerPanel.add(queryPanel);
		centerPanel.add(resultPanel);
		this.add(centerPanel);
//		this.add(headerPanel, BorderLayout.NORTH);
		this.setVisible(true);
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
	
	public void createComboBox() {
        loadDatabases();

        if (databases == null || databases.isEmpty()) {
            JPanel errPanel = new JPanel();
            
            errPanel.add(new JLabel("No databases exist!"));
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
        	public void actionPerformed(ActionEvent ae) {
        		createTableChoices(cbDb.getSelectedItem().toString());
        	}
        });
        
        createTableChoices(stringDb[0]);
//        headerPanel.removeAll();
        headerPanel.add(cbDb);
        headerPanel.add(cbTable);
        add(headerPanel, BorderLayout.NORTH);

        JPanel executePanel = new JPanel();
        executePanel.setBackground(colors.get(0));
        executeButton = new JButton("Execute");
        
        executeButton.addActionListener(new ActionListener() {
        	@Override
        	public void actionPerformed(ActionEvent ae) {
        		String message = buildMessage();
        		if (!message.isEmpty()) {
        			executeSelect(message);
        		}
        	}
        });
        
        executePanel.add(executeButton);
        add(executePanel, BorderLayout.SOUTH);
        
        revalidate();
        repaint();
    }
	
	public void createTableChoices(String db) {
        headerPanel.remove(cbTable);
        ArrayList<String> ownTables = dbTables.get(db);
        if (ownTables == null || ownTables.isEmpty()) {
            headerPanel.remove(cbTable);
            headerPanel.revalidate();
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
        headerPanel.add(cbTable);
        headerPanel.revalidate();

        revalidate();
        repaint();
    }
	
	private void initializeOutputPanel() {
		resultPanel = new JPanel(new BorderLayout());
		resultPanel.setBorder(new EmptyBorder(20, 20, 20, 20));
		resultPanel.setBackground(colors.get(0));
		
		tablePanel = new JPanel();
		tablePanel.setBackground(colors.get(1));
		JScrollPane tableScrollPane = new JScrollPane(tablePanel);
		resultPanel.add(tableScrollPane);
//		tablePanel.setBackground();
		JLabel resultLabel = new JLabel("RESULT");
		resultLabel.setForeground(colors.get(2));
		resultPanel.add(resultLabel, BorderLayout.NORTH);
		
		answerLabel = new JLabel("");
		answerLabel.setForeground(colors.get(2));
		resultPanel.add(answerLabel, BorderLayout.SOUTH);
	}
	
	private void initializeJoinPanel() {
		joinPanel = new JPanel(new BorderLayout());
		joinPanel.setBorder(new EmptyBorder(20, 20, 20, 20));
		joinPanel.setBackground(colors.get(0));
		JLabel joinLabel = new JLabel("JOIN");
		joinLabel.setForeground(colors.get(2));
		joinPanel.add(joinLabel, BorderLayout.NORTH);
		
		JPanel tablePanel = new JPanel(new BorderLayout());
		JPanel fieldNamePanel = new JPanel(new GridLayout(1, 4));
		fieldNamePanel.setBackground(colors.get(1));
		String[] fieldNames = {"Table", "ON", "Attribute", " = ", "Attribute"};
		for (String fieldName : fieldNames) {
			JLabel fieldNameLabel = new JLabel(fieldName, SwingConstants.CENTER);
//			fieldNameLabel.setBackground(colors.get(1));
			fieldNameLabel.setForeground(colors.get(2));
			fieldNamePanel.add(fieldNameLabel);
		}
		
		joinTable = new JTable(7, 5) {
			@Override
			public boolean isCellEditable(int row, int col) {
				return col != 1 && col != 3;
			}
		};
		joinTable.setRowHeight(20);
		joinTable.setBackground(colors.get(0));
		joinTable.setForeground(colors.get(2));
		
		for (int i = 0; i < 7; i++) {
			joinTable.setValueAt("ON", i, 1);
			joinTable.setValueAt(" = ", i, 3);
		}
		
		tablePanel.setBackground(colors.get(0));
		tablePanel.add(joinTable, BorderLayout.CENTER);
		tablePanel.add(fieldNamePanel, BorderLayout.NORTH);
		JScrollPane joinScrollPane = new JScrollPane(tablePanel);
		joinScrollPane.getVerticalScrollBar().setUI(new BasicScrollBarUI() {
			@Override
			protected void configureScrollBarColors() {
				this.thumbColor = colors.get(2);
				this.trackColor = colors.get(1);
			}
		});
		joinScrollPane.getHorizontalScrollBar().setUI(new BasicScrollBarUI() {
			@Override
			protected void configureScrollBarColors() {
				this.thumbColor = colors.get(2);
				this.trackColor = colors.get(1);
			}
		});
		joinPanel.add(joinScrollPane);
	}

	private void initializeWherePanel() {
		wherePanel = new JPanel(new BorderLayout());
		wherePanel.setBorder(new EmptyBorder(20, 20, 20, 20));
		wherePanel.setBackground(colors.get(0));
		JLabel whereLabel = new JLabel("WHERE");
		whereLabel.setForeground(colors.get(2));
		wherePanel.add(whereLabel, BorderLayout.NORTH);
		
		JPanel tablePanel = new JPanel(new BorderLayout());
		JPanel fieldNamePanel = new JPanel(new GridLayout(1, 4));
		fieldNamePanel.setBackground(colors.get(1));
		String[] fieldNames = {"Attribute", "Operator", "Value"};
		for (String fieldName : fieldNames) {
			JLabel fieldNameLabel = new JLabel(fieldName, SwingConstants.CENTER);
//			fieldNameLabel.setBackground(colors.get(1));
			fieldNameLabel.setForeground(colors.get(2));
			fieldNamePanel.add(fieldNameLabel);
		}
		
		whereTable = new JTable(8, 3);
		whereTable.setRowHeight(20);
		whereTable.setBackground(colors.get(0));
		whereTable.setForeground(colors.get(2));
		
		tablePanel.setBackground(colors.get(0));
		tablePanel.add(whereTable, BorderLayout.CENTER);
		tablePanel.add(fieldNamePanel, BorderLayout.NORTH);
		JScrollPane whereScrollPane = new JScrollPane(tablePanel);
		whereScrollPane.getVerticalScrollBar().setUI(new BasicScrollBarUI() {
			@Override
			protected void configureScrollBarColors() {
				this.thumbColor = colors.get(2);
				this.trackColor = colors.get(1);
			}
		});
		whereScrollPane.getHorizontalScrollBar().setUI(new BasicScrollBarUI() {
			@Override
			protected void configureScrollBarColors() {
				this.thumbColor = colors.get(2);
				this.trackColor = colors.get(1);
			}
		});
		wherePanel.add(whereScrollPane);;
	}
	
	private void initializeSelectPanel() {
		JPanel wrapperPanel = new JPanel(new BorderLayout());
		
		selectPanel = new JPanel(new BorderLayout());
		selectPanel.setBorder(new EmptyBorder(20, 20, 20, 20));
		selectPanel.setBackground(colors.get(0));
		JLabel selectLabel = new JLabel("SELECT");
		selectLabel.setForeground(colors.get(2));
		selectPanel.add(selectLabel, BorderLayout.NORTH);
		
		selectCheckBox = new JCheckBox("All Attributes");
		selectCheckBox.setBackground(colors.get(0));
		selectCheckBox.setForeground(colors.get(2));
		wrapperPanel.add(selectCheckBox, BorderLayout.NORTH);
		
		
		JPanel tablePanel = new JPanel(new BorderLayout());
		JPanel fieldNamePanel = new JPanel(new GridLayout(1, 4));
		fieldNamePanel.setBackground(colors.get(1));
		String[] fieldNames = {"Attribute", "Display As"};
		for (String fieldName : fieldNames) {
			JLabel fieldNameLabel = new JLabel(fieldName, SwingConstants.CENTER);
//			fieldNameLabel.setBackground(colors.get(1));
			fieldNameLabel.setForeground(colors.get(2));
			fieldNamePanel.add(fieldNameLabel);
		}
		
		selectTable = new JTable(7, 2);
		selectTable.setRowHeight(20);
		selectTable.setBackground(colors.get(0));
		selectTable.setForeground(colors.get(2));
		
		tablePanel.setBackground(colors.get(0));
		tablePanel.add(selectTable, BorderLayout.CENTER);
		tablePanel.add(fieldNamePanel, BorderLayout.NORTH);
		JScrollPane selectScrollPane = new JScrollPane(tablePanel);
		selectScrollPane.getVerticalScrollBar().setUI(new BasicScrollBarUI() {
			@Override
			protected void configureScrollBarColors() {
				this.thumbColor = colors.get(2);
				this.trackColor = colors.get(1);
			}
		});
		selectScrollPane.getHorizontalScrollBar().setUI(new BasicScrollBarUI() {
			@Override
			protected void configureScrollBarColors() {
				this.thumbColor = colors.get(2);
				this.trackColor = colors.get(1);
			}
		});
		wrapperPanel.add(selectScrollPane, BorderLayout.CENTER);
		selectPanel.add(wrapperPanel, BorderLayout.CENTER); 
	}
	
	private String buildMessage() {
		StringBuilder request = new StringBuilder("USE " + cbDb.getSelectedItem().toString() + "\n");
		request.append("SELECT ");
		if (!selectCheckBox.isSelected()) {
			String parts = "";
			for (int i = 0; i < 6; i++) {
				String actValue = (String) selectTable.getValueAt(i, 0);
				if (actValue != null && !actValue.isEmpty()) {
					parts = parts + actValue;
					String actAlias = (String) selectTable.getValueAt(i, 1);
					if (actAlias != null && !actAlias.isEmpty()) {
						parts = parts + " AS " + actAlias;
					}
					parts = parts + ",";
				}
				
			}
			if (parts.isEmpty()) {
				answerLabel.setText("Choose fields to select!");
				answerLabel.setForeground(Color.RED);
				return "";
			}
			request.append(parts.substring(0, parts.length() - 1) + "\n");
		}
		else {
			request.append("*\n");
		}
		request.append("FROM " + cbTable.getSelectedItem().toString() + "\n");
		
		for (int i = 0; i < 7; i++) {
			String actTable = (String) joinTable.getValueAt(i, 0);
			String actAttr = (String) joinTable.getValueAt(i, 2);
			String otherAttr = (String) joinTable.getValueAt(i, 4);
		
			if (actTable == null || actTable.isEmpty() ||
					actAttr == null || actAttr.isEmpty() ||
					otherAttr == null || otherAttr.isEmpty()) {
				continue;
			}
			request.append("JOIN " + actTable + " ON " + actAttr + " = " + otherAttr + "\n");
		}
		
		boolean firstPart = true;
		String parts = "WHERE ";
		for (int i = 0; i < 8; i++) {
			String value1 = (String) whereTable.getValueAt(i, 0);
			String operator = (String) whereTable.getValueAt(i, 1);
			String value2 = (String) whereTable.getValueAt(i, 2);
			if (value1 == null || value1.isEmpty() ||
					operator == null || operator.isEmpty() ||
					value2 == null || value2.isEmpty()) {
				continue;
			}
			if (!firstPart) {
				parts = parts + "AND ";
			}
			parts = parts + value1 + " " + operator + " " + value2 + " ";
			firstPart = false;
		}
		if (!firstPart) {
			request.append(parts);
		}
		
		System.out.println(request);
		return request.toString();
	}

	private void executeSelect(String request) {
		try {
			Socket socket = new Socket("localhost", 12000);
			ClientFrame.sendRequest(socket, request);
			receiveAnswer(socket);
		} catch (IOException e) {
			answerLabel.setForeground(Color.RED);
			answerLabel.setText("Error: Can't connect to server!");
		}
	}
	
	private void receiveAnswer(Socket socket) throws IOException {
		BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		JLabel fieldNameLabel = null;
		JPanel fieldNamePanel = null;
		String answerReceived;
		JTable table = null;
		while (true) {
			answerReceived = socketReader.readLine();
			if (answerReceived.equals(ValidatorDictionary.ok)) {
				answerLabel.setForeground(Color.GREEN);
				answerLabel.setText(answerReceived);
				break;
			} else if (answerReceived.matches("^\\d+$")) {
				answerLabel.setForeground(Color.GREEN);
				int rowNr = Integer.parseInt(answerReceived);
				answerReceived = socketReader.readLine();
				int colNr = Integer.parseInt(answerReceived);

				table = new JTable(rowNr, colNr);
				table.setDefaultEditor(Object.class, null); // set ineditable
				fieldNamePanel = new JPanel(new GridLayout(1, colNr));
				answerReceived = socketReader.readLine(); // field names = header
				String[] fieldNames = answerReceived.split(",");
				for (String fieldName : fieldNames) {
					fieldNameLabel = new JLabel(fieldName, SwingConstants.CENTER);
					fieldNameLabel.setForeground(colors.get(2));
					fieldNamePanel.add(fieldNameLabel);
				}

				for (int i = 0; i < rowNr; i++) {
					answerReceived = socketReader.readLine();
					String[] tmp = answerReceived.split(",");
					String[] answerParts = ValidatorDictionary.partsHotfix(tmp);
					int col_ind = 0;
					for (String answerPart : answerParts) {
						table.setValueAt(answerPart, i, col_ind);
						col_ind++;
					}
				}

			} else {
				answerLabel.setForeground(Color.RED);
				answerLabel.setText(answerReceived);
				break;
			}
		}
		if (answerReceived.equals(ValidatorDictionary.ok) && table != null) {
			tablePanel.removeAll();
			tablePanel.setLayout(new BorderLayout());
			

//			JPanel oneTablePanel = new JPanel(new BorderLayout());
			tablePanel.setBorder(new EmptyBorder(10, 0, 10, 0));
			tablePanel.add(fieldNamePanel, BorderLayout.NORTH);
			tablePanel.add(table, BorderLayout.CENTER);
			tablePanel.setBackground(colors.get(1));
			
			fieldNamePanel.setBackground(colors.get(0));
			
			table.setBackground(colors.get(1));
			table.setForeground(colors.get(2));
			tablePanel.revalidate();
			tablePanel.repaint();
			
		}
//		tablePanel.revalidate();
//		tablePanel.repaint();
//		selectPane.revalidate();
//		selectPane.repaint();
	}
	
	public static void main(String[] args) {
		new SelectFrame();
	}
}
