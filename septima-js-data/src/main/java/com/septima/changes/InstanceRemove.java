package com.septima.changes;

import java.util.Map;

/**
 * @author mg
 */
public class InstanceRemove extends EntityAction {

    private final Map<String, Object> keys;

    public InstanceRemove(String aEntityName, Map<String, Object> aKeys) {
        super(aEntityName);
        keys = aKeys;
    }

    @Override
    public void accept(EntityActionsVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    public Map<String, Object> getKeys() {
        return keys;
    }

}
