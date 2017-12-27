package com.septima.changes;

/**
 * @author mg
 */
public abstract class EntityAction {

    private final String entityName;

    public EntityAction(String aEntityName) {
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

    public abstract void accept(EntityActionsVisitor aChangeVisitor);
}
