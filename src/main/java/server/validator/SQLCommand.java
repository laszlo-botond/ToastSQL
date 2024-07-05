package server.validator;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLCommand {
    private final ArrayList<SQLCommand> children;
    private final ArrayList<String> acceptable;
    private final boolean acceptAnyName;
    
    private boolean okAsLast;
    private boolean numberNeeded;
    private boolean canGetString;
    private boolean isCmdSeparator;
    private boolean isCustomRequest;
    private boolean forceRedirect;
    private boolean canHaveDot; 
    
    private String beginMsg;
    
    public SQLCommand(boolean shouldAcceptAny) {
        acceptable = new ArrayList<>();
        children = new ArrayList<>();
        acceptAnyName = shouldAcceptAny;
        okAsLast = false;
        numberNeeded = false;
        isCmdSeparator = false;
        canGetString = false;
        forceRedirect = false;
        canHaveDot = false;
        beginMsg = "";
    }

    public void setAsSeparator() {
        isCmdSeparator = true;
    }

    public void setAsCustomRequest(String beginMsg) {
        isCustomRequest = true;
        this.beginMsg = beginMsg;
    }

    public void setOkAsLast() {
        okAsLast = true;
    }

    public void needsNumber() {
        numberNeeded = true;
    }
    
    public void allowString() {
        canGetString = true;
    }

	public void allowDot() {
        canHaveDot = true;
    }

    public void addChild(SQLCommand sqlc) {
        children.add(sqlc);
    }

    public void addAcceptablePhrase(String phrase) {
        acceptable.add(phrase);
    }
    
    public void setForceRedirect() {
        forceRedirect = true;
    }

    public boolean verify(String command, String thisLine, ArrayList<String> commandLines) {
        boolean match = false;
        String beginning = "";
        
		// user-defined word accepted
        if (acceptAnyName) { 
            
            // can get string with quotation marks
        	if (canGetString && command.charAt(0) == '\"') {
                
	            try {
	            	beginning = "\"" + command.substring(1).split("\"")[0] + "\"";
	            } catch(IndexOutOfBoundsException e) {
	            	return false;
	            }
	            
	            // no closing quotation mark
	            if (command.substring(1).length() == command.substring(1).split("\"")[0].length()) {
	                return false;
	            }
	            
	            match = true;
	            
	        // accepts anything but doesn't need string
            } else {
	            try {
                    String splitRegex = "[ .,()=<>\\+\\*]";
                    if (canHaveDot) {
                        splitRegex = "[ ,()=<>\\+\\*]";
                    }
	            	beginning = command.split(splitRegex)[0];
	            } catch(IndexOutOfBoundsException e) {
	            	return false;
	            }
	            
	            // non-string can't contain quotation mark
	            if (beginning.contains("\"")) {
                	return false;
            	}
            	
            	if (beginning.isEmpty()) return false;
            	match = true;
            }
            
        // needs user-defined integer
        } else if (numberNeeded) { 
            Pattern pattern = Pattern.compile("^\\d+");
            Matcher matcher = pattern.matcher(command);
            if (matcher.find()) {
                beginning = matcher.group();
                match = true;
            }
            
        // required syntax
        } else { 
            for (String allowed : acceptable) {
                if (command.toUpperCase().startsWith(allowed)) {
                    match = true;
                    beginning = allowed;
                    break;
                }
            }
        }

        if (!match) return false; // matched this pattern

        // System.out.println("Beginning: " + beginning);
        
        String rem = command.substring(beginning.length());

        if (isCustomRequest) {
            beginning = beginMsg + beginning;
        }
        
        String nextLine;
        String thisLineBackup = thisLine;
        if (isCmdSeparator) {
            thisLine = thisLine + "," + beginning;
            commandLines.add(thisLine);
            nextLine = "";
        } else if (thisLine.isEmpty()){
            nextLine = beginning;
        } else {
            nextLine = thisLine + "," + beginning;
        }

        // cut off beginning spaces
        while (rem.startsWith(" ")) {
            rem = rem.substring(1);
        }

        // command reached end
        if (rem.isEmpty() && !forceRedirect) return okAsLast; // command ended correctly

        // rest of command needs to be verified
        for (SQLCommand child : children) {
            if (child.verify(rem, nextLine, commandLines)) {
                return true;
            }
        }

        // didn't match any
        if (isCmdSeparator) {
            commandLines.remove(commandLines.size() - 1);
        }
        thisLine = thisLineBackup;
        return false;
    }

}
