package com.septima.changes;

import java.util.Map;

/**
 * @author mg
 */
public class Command extends Change {

    private final Map<String, Object> arguments;

    public Command(final String anEntityName, final Map<String, Object> aParameters) {
        super(anEntityName);
        arguments = aParameters;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    @Override
    public void accept(ChangesVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

}
