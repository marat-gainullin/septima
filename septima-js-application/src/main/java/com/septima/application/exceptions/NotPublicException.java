package com.septima.application.exceptions;

public class NotPublicException extends NoAccessException {

    private final static long serialVersionUID = 1L;

    public NotPublicException(String anEntityName) {
        super("Public access to '" + anEntityName + "' is not allowed");
    }
}
