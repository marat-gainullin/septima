package com.septima.changes;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author mg
 */
public class CommandRequest extends Change implements Change.Transferable {

    private final Map<String, NamedValue> parameters = new HashMap<>();

    public CommandRequest(final String entityName) {
        super(entityName);
    }

    public Map<String, NamedValue> getParameters() {
        return parameters;
    }

    @Override
    public void accept(TransferableChangeVisitor aChangeVisitor) throws Exception {
        aChangeVisitor.visit(this);
    }

}
