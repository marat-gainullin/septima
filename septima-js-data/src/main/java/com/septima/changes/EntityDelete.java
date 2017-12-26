package com.septima.changes;

import java.util.Map;

/**
 * @author mg
 */
public class EntityDelete extends EntityChange {

    private final Map<String, Object> keys;

    public EntityDelete(String aEntityName, Map<String, Object> aKeys) {
        super(aEntityName);
        keys = aKeys;
    }

    @Override
    public void accept(EntityChangesVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    public Map<String, Object> getKeys() {
        return keys;
    }

}
