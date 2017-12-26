package com.septima.changes;

/**
 * @author mg
 */
public abstract class EntityChange {

    private final String entityName;

    public EntityChange(String aEntityName) {
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

    public abstract void accept(EntityChangesVisitor aChangeVisitor);
}
