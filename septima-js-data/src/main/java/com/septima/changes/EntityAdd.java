package com.septima.changes;

import java.util.Map;

/**
 * @author mg
 */
public class EntityAdd extends EntityAction {

    private final Map<String, Object> data;

    public EntityAdd(String aEntityName, Map<String, Object> aData) {
        super(aEntityName);
        data = aData;
    }

    @Override
    public void accept(EntityActionsVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    public Map<String, Object> getData() {
        return data;
    }

}
