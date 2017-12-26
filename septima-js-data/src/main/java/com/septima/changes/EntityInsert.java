package com.septima.changes;

import java.util.Map;

/**
 * @author mg
 */
public class EntityInsert extends EntityChange {

    private final Map<String, Object> data;

    public EntityInsert(String aEntityName, Map<String, Object> aData) {
        super(aEntityName);
        data = aData;
    }

    @Override
    public void accept(EntityChangesVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    public Map<String, Object> getData() {
        return data;
    }

}
