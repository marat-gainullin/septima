package com.septima.changes;

/**
 *
 * @author mg
 */
public interface EntityChangesVisitor {

    void visit(EntityInsert aChange) ;

    void visit(EntityUpdate aChange) ;

    void visit(EntityDelete aChange) ;

    void visit(EntityCommand aChange) ;
}
