package com.septima.changes;

import java.util.Map;

/**
 * @author mg
 */
public class Delete extends Change {

    private final Map<String, Object> keys;

    public Delete(String aEntityName, Map<String, Object> aKeys) {
        super(aEntityName);
        keys = aKeys;
    }

    @Override
    public void accept(ChangesVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    public Map<String, Object> getKeys() {
        return keys;
    }

}
