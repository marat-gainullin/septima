package com.septima.changes;

/**
 * @author mg
 */
public abstract class Change {

    private final String entityName;

    public Change(String aEntityName) {
        super();
        entityName = aEntityName;
    }

    public String getEntityName() {
        return entityName;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public abstract void accept(ChangesVisitor aChangeVisitor);
}
