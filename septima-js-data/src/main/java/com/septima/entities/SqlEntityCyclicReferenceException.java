package com.septima.entities;

public class SqlEntityCyclicReferenceException extends RuntimeException {

    private static final long serialVersionUID = -7214325061535242366L;

    public SqlEntityCyclicReferenceException(String anEntityName) {
        super("Cyclic reference detected with entity: " + anEntityName);
    }
}
