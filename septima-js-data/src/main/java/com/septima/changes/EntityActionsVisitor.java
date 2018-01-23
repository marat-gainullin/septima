package com.septima.changes;

/**
 *
 * @author mg
 */
public interface EntityActionsVisitor {

    void visit(InstanceAdd aAdd);

    void visit(InstanceChange aChange);

    void visit(InstanceRemove aRemove);

    void visit(EntityCommand aCommand);
}
