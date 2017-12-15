package com.septima.changes;

import com.septima.NamedValue;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mg
 */
public class Insert extends Change implements Change.Applicable, Change.Transferable {

    private final List<NamedValue> data = new ArrayList<>();

    public Insert(String aEntityName) {
        super(aEntityName);
    }

    @Override
    public void accept(TransferableChangeVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    @Override
    public void accept(ApplicableChangeVisitor aChangeVisitor) {
        aChangeVisitor.visit(this);
    }

    public List<NamedValue> getData() {
        return data;
    }

}
