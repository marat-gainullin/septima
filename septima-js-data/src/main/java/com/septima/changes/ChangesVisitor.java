package com.septima.changes;

/**
 *
 * @author mg
 */
public interface ChangesVisitor {

    void visit(Insert aChange) ;

    void visit(Update aChange) ;

    void visit(Delete aChange) ;

    void visit(Command aChange) ;
}
