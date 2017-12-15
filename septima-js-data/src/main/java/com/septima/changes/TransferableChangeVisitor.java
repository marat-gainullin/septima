package com.septima.changes;

/**
 *
 * @author mg
 */
public interface TransferableChangeVisitor {

    void visit(Insert aChange) ;

    void visit(Update aChange) ;

    void visit(Delete aChange) ;

    void visit(CommandRequest aChange) ;
}
