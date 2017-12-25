package com.septima.changes;

import java.util.Map;

/**
 * @author mg
 */
public class Insert extends Change {

    private final Map<String, Object> data;

    public Insert(String aEntityName, Map<String, Object> aData) {
        super(aEntityName);
        data = aData;
    }

    @Override
    public void accept(ChangesVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    public Map<String, Object> getData() {
        return data;
    }

}
