package com.septima.changes;

import com.septima.NamedValue;

import java.util.Map;

/**
 * @author mg
 */
public class Command extends Change {

    private final Map<String, NamedValue> parameters;

    public Command(final String anEntityName, final Map<String, NamedValue> aParameters) {
        super(anEntityName);
        parameters = aParameters;
    }

    public Map<String, NamedValue> getParameters() {
        return parameters;
    }

    @Override
    public void accept(ChangesVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

}
