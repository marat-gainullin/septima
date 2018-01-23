package com.septima.changes;

import java.util.Map;

/**
 * @author mg
 */
public class InstanceChange extends EntityAction {

    private final Map<String, Object> keys;
    private final Map<String, Object> data;

    public InstanceChange(String aEntityName, Map<String, Object> aKeys, Map<String, Object> aData) {
        super(aEntityName);
        keys = aKeys;
        data = aData;
    }

    @Override
    public void accept(EntityActionsVisitor aActionsVisitor) {
        aActionsVisitor.visit(this);
    }

    public Map<String, Object> getKeys() {
        return keys;
    }

    public Map<String, Object> getData() {
        return data;
    }

}
