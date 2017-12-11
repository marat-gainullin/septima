package com.septima.changes;

/**
 *
 * @author mg
 */
public interface ApplicableChangeVisitor {

    void visit(Insert aChange) throws Exception;

    void visit(Update aChange) throws Exception;

    void visit(Delete aChange) throws Exception;

    void visit(Command aChange) throws Exception;
}
