package com.septima.changes;

/**
 *
 * @author mg
 */
public interface EntityActionsVisitor {

    void visit(EntityAdd aAdd);

    void visit(EntityChange aChange);

    void visit(EntityRemove aRemove);

    void visit(EntityCommand aCommand);
}
