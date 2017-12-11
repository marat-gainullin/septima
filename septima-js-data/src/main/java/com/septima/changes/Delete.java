package com.septima.changes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mg
 */
public class Delete extends Change implements Change.Applicable, Change.Transferable {

    private final List<NamedValue> keys = new ArrayList<>();

    public Delete(String aEntityName) {
        super(aEntityName);
    }

    @Override
    public void accept(TransferableChangeVisitor aChangeVisitor) throws Exception {
        aChangeVisitor.visit(this);
    }

    @Override
    public void accept(ApplicableChangeVisitor aChangeVisitor) throws Exception {
        aChangeVisitor.visit(this);
    }

    public List<NamedValue> getKeys() {
        return keys;
    }

}
