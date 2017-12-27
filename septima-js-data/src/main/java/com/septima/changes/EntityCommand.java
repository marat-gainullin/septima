package com.septima.changes;

import java.util.Map;

/**
 * @author mg
 */
public class EntityCommand extends EntityAction {

    private final Map<String, Object> arguments;

    public EntityCommand(final String anEntityName, final Map<String, Object> aParameters) {
        super(anEntityName);
        arguments = aParameters;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    @Override
    public void accept(EntityActionsVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

}
