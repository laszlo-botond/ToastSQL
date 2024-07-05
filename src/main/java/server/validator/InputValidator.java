package server.validator;

import java.util.ArrayList;

public class InputValidator {
    private final SQLCommand validatorRoot;
    private ArrayList<String> commandLines;
    public InputValidator() {
        commandLines = new ArrayList<>();

        commandLines = new ArrayList<>();

        validatorRoot = new SQLCommand(false);
        validatorRoot.addAcceptablePhrase("");

        GraphBuilder.addCreateDropNodes(validatorRoot);
        GraphBuilder.addInsertNodes(validatorRoot);
        GraphBuilder.addDeleteNodes(validatorRoot);
        GraphBuilder.addShowNodes(validatorRoot);
        GraphBuilder.addSelectNodes(validatorRoot);
    }

    public boolean validate(String command) {
        ArrayList<String> cl = new ArrayList<>();
        boolean ok = validatorRoot.verify(command, "", cl);
        commandLines = cl;
        return ok;
    }

    public ArrayList<String> getCommandLines() {
        return commandLines;
    }
}
