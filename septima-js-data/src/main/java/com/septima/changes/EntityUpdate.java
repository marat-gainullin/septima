package com.septima.changes;

import java.util.Map;

/**
 * @author mg
 */
public class EntityUpdate extends EntityChange {

    private final Map<String, Object> keys;
    private final Map<String, Object> data;

    public EntityUpdate(String aEntityName, Map<String, Object> aKeys, Map<String, Object> aData) {
        super(aEntityName);
        keys = aKeys;
        data = aData;
    }

    @Override
    public void accept(EntityChangesVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    public Map<String, Object> getKeys() {
        return keys;
    }

    public Map<String, Object> getData() {
        return data;
    }

}
