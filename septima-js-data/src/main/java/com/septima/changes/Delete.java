package com.septima.changes;

import com.septima.NamedValue;

import java.util.List;

/**
 * @author mg
 */
public class Delete extends Change {

    private final List<NamedValue> keys;

    public Delete(String aEntityName, List<NamedValue> aKeys) {
        super(aEntityName);
        keys = aKeys;
    }

    @Override
    public void accept(ChangesVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    public List<NamedValue> getKeys() {
        return keys;
    }

}
