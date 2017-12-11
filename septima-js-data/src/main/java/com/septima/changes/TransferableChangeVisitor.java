package com.septima.changes;

/**
 *
 * @author mg
 */
public interface TransferableChangeVisitor {

    void visit(Insert aChange) throws Exception;

    void visit(Update aChange) throws Exception;

    void visit(Delete aChange) throws Exception;

    void visit(CommandRequest aChange) throws Exception;
}
