package com.septima.application.exceptions;

public class NoImplementationException extends EndPointException {
    private final static long serialVersionUID = 1L;

    public NoImplementationException() {
        super("Not implemented");
    }
}
