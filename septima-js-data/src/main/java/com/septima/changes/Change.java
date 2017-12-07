package com.septima.changes;

/**
 * @author mg
 */
public abstract class Change {

    public interface Generic {

        String getEntity();
    }

    /**
     * Changes marked with this interface are applicable to a datasource.
     *
     * @author mgainullin
     */
    public interface Applicable extends Generic {

        void accept(ApplicableChangeVisitor aChangeVisitor) throws Exception;
    }

    /**
     * Changes marked with this interface are transferable over the network.
     *
     * @author mgainullin
     */
    public interface Transferable extends Generic {

        void accept(TransferableChangeVisitor aChangeVisitor) throws Exception;
    }

    private final String entityName;
    public boolean consumed;

    public Change(String aEntityName) {
        super();
        entityName = aEntityName;
    }

    public String getEntity() {
        return entityName;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
