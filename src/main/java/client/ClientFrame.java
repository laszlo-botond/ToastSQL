package client;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import javax.imageio.ImageIO;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.SwingConstants;
import javax.swing.border.EmptyBorder;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.plaf.basic.BasicScrollBarUI;

import server.validator.ValidatorDictionary;

public class ClientFrame extends JFrame {
	private final JTextArea commandInputArea;
	private JLabel answerLabel;
	private String answerReceived;
	private JPanel sidePanel;
	private String prefixSpace;
	private JPanel tablePanel;
	private JScrollPane selectPane;
	private JPanel logoPanel;
	
	
	private static Color color1;
	private static Color color2;
	private static Color color3;
	private static Color color4;
	private static BufferedImage img;
	
	private List<JTable> tablesList;
	private List<JPanel> headerPanelList;

	public ClientFrame() {
		this.setBounds(100, 100, 800, 800);
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);
		this.setTitle("ToastSQL Client");
		
		try {
			img = ImageIO.read(getClass().getResource("/img/ToastLogo.png"));
			this.setIconImage(img);
		} catch (IOException e1) {
			img = null;
		}
		
		JPanel mainPanel = new JPanel();
		GridLayout gLayout = new GridLayout(2, 1);
		mainPanel.setLayout(gLayout);

		JPanel textPanel = new JPanel(new BorderLayout());
		textPanel.setBorder(new EmptyBorder(0, 20, 20, 20));
		commandInputArea = new JTextArea();
		commandInputArea.setFont(new Font("Arial", Font.PLAIN, 18));
		commandInputArea.setTabSize(3);
		JScrollPane scrollPane = new JScrollPane(commandInputArea);

		textPanel.add(scrollPane, BorderLayout.CENTER);
		answerLabel = new JLabel();
		textPanel.add(answerLabel, BorderLayout.SOUTH);
		mainPanel.add(textPanel);

		JPanel bottomPanel = new JPanel(new BorderLayout());
		bottomPanel.setBorder(new EmptyBorder(20, 20, 20, 20));
		bottomPanel.setBackground(Color.BLACK);
		tablePanel = new JPanel(new GridLayout())/*{
			@Override
			public void paintComponent(Graphics g) {
				super.paintComponent(g);
				BufferedImage img;
				try {
					img = ImageIO.read(getClass().getResource("/img/Duke.png"));
				} catch (IOException e1) {
					img = null;
				}
				if (img != null) {
					g.drawImage(img, 0, getHeight() - 100, 100, 100, null);
				}
			}
		}*/;
		selectPane = new JScrollPane(tablePanel);
//		selectPane.setBackground(Color.RED);
		bottomPanel.add(selectPane);
		mainPanel.add(bottomPanel);

		JMenuBar menuBar = new JMenuBar();
		JMenu fileMenu = new JMenu("File");
		JFileChooser fileManager = new JFileChooser();
		fileManager.setFileFilter(new FileNameExtensionFilter("SQL Files", "sql", "sql"));

		JMenuItem save = new JMenuItem("Save");
		save.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ae) {
				int ret = fileManager.showSaveDialog(null);
				if (ret == JFileChooser.APPROVE_OPTION) {
					try {
						FileWriter fw = new FileWriter(fileManager.getSelectedFile());
						fw.write(commandInputArea.getText());
						fw.flush();
						fw.close();
					} catch (IOException e) {
						System.out.println("Error when saving file!");
					}
				}
			}
		});

		JMenuItem load = new JMenuItem("Load");
		load.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ae) {
				int ret = fileManager.showOpenDialog(null);
				if (ret == JFileChooser.APPROVE_OPTION) {
					try {
						FileReader file = new FileReader(fileManager.getSelectedFile());
						char[] cbuf = new char[1024];
						StringBuilder cmdLoader = new StringBuilder();
						while (true) {
							int byteNr = file.read(cbuf);
							if (byteNr < 0) {
								break;
							}
							cmdLoader.append(cbuf, 0, byteNr);
						}
						commandInputArea.setText(cmdLoader.toString());

					} catch (IOException e) {
						System.out.println("Error when opening file!");
					}

				}
			}
		});

		JMenuItem exec = new JMenuItem("Execute");
		fileMenu.add(save);
		fileMenu.add(load);
		menuBar.add(fileMenu);
		menuBar.add(exec);
		exec.setForeground(new Color(0, 150, 0));
		exec.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				ClientFrame.this.execute();
			}
		});

		JMenuItem insertItem = new JMenuItem("Insert");
		JMenuItem deleteItem = new JMenuItem("Delete");
		JMenuItem selectItem = new JMenuItem("Select");
		JButton themeButton = new JButton("Dark");
		themeButton.setPreferredSize(new Dimension(66, 25));

		insertItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent ae) {
				new InsertFrame();
			}
		});
		selectItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent ae) {
				new SelectFrame();
			}
		});
		deleteItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent ae) {
				new DeleteFrame();
			}
		});

		themeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent ae) {

				if (((JButton) ae.getSource()).getText().equals("Dark")) {
					color1 = new Color(60, 60, 60);
					color2 = new Color(30, 30, 30);
					color3 = Color.WHITE;
					color4 = new Color(191, 255, 0);
					((JButton) ae.getSource()).setText("Light");
				} else {
					color1 = new Color(240, 240, 240);
					color2 = Color.WHITE;
					color3 = Color.BLACK;
					color4 = new Color(0, 150, 0);
					((JButton) ae.getSource()).setText("Dark");
				}
				mainPanel.setBackground(color1);
				textPanel.setBackground(color1);
				bottomPanel.setBackground(color1);
				tablePanel.setBackground(color2);
				commandInputArea.setBackground(color2);
				commandInputArea.setForeground(color3);
				commandInputArea.setCaretColor(color3);
				sidePanel.setBackground(color2);
				logoPanel.setBackground(color1);
				Arrays.stream(sidePanel.getComponents()).forEach(lbl -> lbl.setForeground(color3));

				if (tablesList != null) {
					tablesList.stream().forEach(t -> {
						t.setBackground(color2); 
						t.setForeground(color3);
						t.getParent().setBackground(color2);
						t.getParent().setForeground(color3);
						});
					headerPanelList.stream().forEach(t -> {
						t.setBackground(color1);
						Arrays.stream(t.getComponents()).forEach(tt -> {
							tt.setBackground(color2);
							tt.setForeground(color3);							
						});
						t.getParent().setBackground(color2);
						t.getParent().setForeground(color3);
						});
				}
				
				tablePanel.setBackground(color2);
				
				
				menuBar.setBackground(color1);
				fileMenu.setBackground(color1);

				selectItem.setBackground(color1);
				insertItem.setBackground(color1);
				deleteItem.setBackground(color1);
				exec.setBackground(color1);

				selectItem.setForeground(color3);
				insertItem.setForeground(color3);
				deleteItem.setForeground(color3);
				exec.setForeground(color4);

				fileMenu.setForeground(color3);
//                scrollPane.getVerticalScrollBar().setBackground(color1);
//                scrollPane.getHorizontalScrollBar().setBackground(color1);

				scrollPane.getVerticalScrollBar().setUI(new BasicScrollBarUI() {
					@Override
					protected void configureScrollBarColors() {
						this.thumbColor = color3;
						this.trackColor = color2;
					}
				});
				scrollPane.getHorizontalScrollBar().setUI(new BasicScrollBarUI() {
					@Override
					protected void configureScrollBarColors() {
						this.thumbColor = color3;
						this.trackColor = color2;
					}
				});
				selectPane.getVerticalScrollBar().setUI(new BasicScrollBarUI() {
					@Override
					protected void configureScrollBarColors() {
						this.thumbColor = color3;
						this.trackColor = color2;
					}
				});
				
				selectPane.getHorizontalScrollBar().setUI(new BasicScrollBarUI() {
					@Override
					protected void configureScrollBarColors() {
						this.thumbColor = color3;
						this.trackColor = color2;
					}
				});

				((JButton) ae.getSource()).setBackground(color2);
				((JButton) ae.getSource()).setForeground(color3);

			}
		});

		menuBar.add(selectItem);
		menuBar.add(insertItem);
		menuBar.add(deleteItem);
		menuBar.add(themeButton);

		
		sidePanel = new JPanel();
		sidePanel.setLayout(new BoxLayout(sidePanel, BoxLayout.Y_AXIS));
		sidePanel.setMinimumSize(new Dimension(180, 800));
		// sidePanel.setPreferredSize(new Dimension(180, 1000));
		sidePanel.setBackground(Color.WHITE);
		JScrollPane sideScrollPane = new JScrollPane(sidePanel);
		sideScrollPane.setPreferredSize(new Dimension(200, 800));

		JPanel sidePanelWrapper = new JPanel(new BorderLayout());
		logoPanel = new JPanel() {
			@Override
			public void paintComponent(Graphics g) {
				super.paintComponent(g);
				if (img != null) {
					g.drawImage(img, 0, 0, 180, 180, null);
				}
			}
		};
		
		logoPanel.setBackground(color1);
		logoPanel.setPreferredSize(new Dimension(180, 180));
 		sidePanelWrapper.add(sideScrollPane);
		sidePanelWrapper.add(logoPanel, BorderLayout.SOUTH);
		this.add(mainPanel, BorderLayout.CENTER);
		this.add(sidePanelWrapper, BorderLayout.WEST);
		this.setJMenuBar(menuBar);

		updateSidePanel();
		themeButton.doClick();
		this.setVisible(true);
		
		
	}

	private void execute() {
		try {
			Socket socket = new Socket("localhost", 12000);

			// kuldes:
			String command = commandInputArea.getSelectedText();
			if (command == null) {
				command = commandInputArea.getText();
			}
//			answerLabel.setText(" ");

			sendRequest(socket, command);
			// valasz fogadasa:
			receiveAnswer(socket);

			socket.close();
		} catch (IOException e) {
			answerLabel.setForeground(Color.RED);
			answerLabel.setText("Error: server is not running!");
			System.out.println("Error: server is not running!");
		}
		updateSidePanel();
	}

	public static void sendRequest(Socket socket, String command) throws IOException {
		PrintStream socketOutput = new PrintStream(socket.getOutputStream());
		command = command.replace("\t", " ");

		command = command.length() + "\n" + command + " ";
		socketOutput.println(command);
	}

	private void receiveAnswer(Socket socket) throws IOException {
		tablesList = new ArrayList<>();
		headerPanelList = new ArrayList<>();
		tablePanel.removeAll();

		BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		JLabel fieldNameLabel;
		answerReceived = socketReader.readLine();
//		System.out.println("Client: " + answerReceived);

		while (true) {
			if (answerReceived.equals(ValidatorDictionary.ok)) {
				answerLabel.setForeground(Color.GREEN);
				answerLabel.setText(answerReceived);
				break;
			} else if (answerReceived.matches("^\\d+$")) {
				answerLabel.setForeground(Color.GREEN);
				int rowNr = Integer.parseInt(answerReceived);
//				System.out.println("Client: " + rowNr);
				answerReceived = socketReader.readLine();
				int colNr = Integer.parseInt(answerReceived);

				JTable table = new JTable(rowNr, colNr);
				table.setDefaultEditor(Object.class, null); // set ineditable
				JPanel fieldNamePanel = new JPanel(new GridLayout(1, colNr));
				answerReceived = socketReader.readLine(); // field names = header
				String[] fieldNames = answerReceived.split(",");
				for (String fieldName : fieldNames) {
					fieldNameLabel = new JLabel(fieldName, SwingConstants.CENTER);
					fieldNameLabel.setForeground(color3);
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

				tablesList.add(table);
				headerPanelList.add(fieldNamePanel);

			} else {
				answerLabel.setForeground(Color.RED);
				answerLabel.setText(answerReceived);
				break;
			}
			
			answerReceived = socketReader.readLine();
		}
		if (answerReceived.equals(ValidatorDictionary.ok) && tablesList != null && tablesList.size() != 0) {
			tablePanel.setLayout(new GridLayout(tablesList.size(), 1));
			
			for (int i = 0; i < tablesList.size(); i++) {
				JPanel oneTablePanel = new JPanel(new BorderLayout());
				oneTablePanel.setBorder(new EmptyBorder(10, 0, 10, 0));
				oneTablePanel.add(headerPanelList.get(i), BorderLayout.NORTH);
				oneTablePanel.add(tablesList.get(i), BorderLayout.CENTER);
				oneTablePanel.setBackground(color2);
				
				headerPanelList.get(i).setBackground(color1);
				
				tablesList.get(i).setBackground(color2);
				tablesList.get(i).setForeground(color3);
				tablePanel.add(oneTablePanel);
			}
		}
		tablePanel.revalidate();
		tablePanel.repaint();
		selectPane.revalidate();
		selectPane.repaint();
	}

	public static void main(String[] args) {
		new ClientFrame();
	}

	private void updateSidePanel() {
		sidePanel.removeAll();
		sidePanel.revalidate();
		sidePanel.repaint();

		prefixSpace = "   ";

		try {
			Socket socket = new Socket("localhost", 12000);
			PrintStream socketOutput = new PrintStream(socket.getOutputStream());
			socketOutput.println("GETALLDBS".length());
			socketOutput.println("GETALLDBS");

			BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			answerReceived = socketReader.readLine();

			Stream.of(answerReceived.split(" ")).forEach(str -> {
				if (str.equals("")) {
					prefixSpace = "   ";
					return;
				}
//            	for (int i = 0; i < 40; i++) {
//            		JLabel dbLabel = new JLabel("PLACEHOLDERASJDHJKASDKPLACEHOLDERASJDHJKASDKPLACEHOLDERASJDHJKASDKPLACEHOLDERASJDHJKASDKPLACEHOLDERASJDHJKASDK");
//                	sidePanel.add(dbLabel);
//            	}
				JLabel dbLabel = new JLabel(prefixSpace + str);
				dbLabel.setForeground(color3);
				sidePanel.add(dbLabel);
				prefixSpace = "       ";
			});
			socketReader.close();
			socketOutput.close();
			socket.close();

		} catch (IOException e) {
			JLabel dbLabel = new JLabel("Couldn't retrieve Databases!");
			dbLabel.setForeground(color3);
			sidePanel.add(dbLabel);
		}
		sidePanel.revalidate();
		sidePanel.repaint();
	}

	public static List<Color> getColors() {
		List<Color> retList = new ArrayList<>();
		retList.add(color1);
		retList.add(color2);
		retList.add(color3);
		retList.add(color4);
		return retList;
	}
	
	public static BufferedImage getIcon() {
		return img;
	}
	
}