package com.septima.application.exceptions;

public class NoCollectionException extends EndPointException {

    private final static long serialVersionUID = 1L;

    public NoCollectionException(String aCollectionRef) {
        super("Collection '" + aCollectionRef + "' is not found.");
    }
}
