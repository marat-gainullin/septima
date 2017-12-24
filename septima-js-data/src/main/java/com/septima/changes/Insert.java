package com.septima.changes;

import com.septima.NamedValue;

import java.util.List;

/**
 * @author mg
 */
public class Insert extends Change {

    private final List<NamedValue> data;

    public Insert(String aEntityName, List<NamedValue> aData) {
        super(aEntityName);
        data = aData;
    }

    @Override
    public void accept(ChangesVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    public List<NamedValue> getData() {
        return data;
    }

}
