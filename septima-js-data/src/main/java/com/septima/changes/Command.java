package com.septima.changes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mg
 */
public class Command extends Change implements Change.Applicable {

    /**
     * Compiled sql clause with linear parameters in form of (?, ?, ?).
     */
    private final String clause;
    /**
     * Compiled and not unique collection of parameters.
     */
    private final List<ChangeValue> parameters = new ArrayList<>();

    public Command(String aEntityName) {
        this(aEntityName, null);
    }

    public Command(String aEntityName, String aClause) {
        super(aEntityName);
        clause = aClause;
    }

    public String getCommand() {
        return clause;
    }

    @Override
    public void accept(ApplicableChangeVisitor aChangeVisitor) throws Exception {
        aChangeVisitor.visit(this);
    }

    public List<ChangeValue> getParameters() {
        return parameters;
    }

}
