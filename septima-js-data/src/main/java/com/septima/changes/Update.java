package com.septima.changes;

import com.septima.NamedValue;

import java.util.List;

/**
 * @author mg
 */
public class Update extends Change {

    private final List<NamedValue> keys;
    private final List<NamedValue> data;

    public Update(String aEntityName, List<NamedValue> aKeys, List<NamedValue> aData) {
        super(aEntityName);
        keys = aKeys;
        data = aData;
    }

    @Override
    public void accept(ChangesVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    public List<NamedValue> getKeys() {
        return keys;
    }

    public List<NamedValue> getData() {
        return data;
    }

}
