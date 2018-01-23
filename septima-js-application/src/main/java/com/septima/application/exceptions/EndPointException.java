package com.septima.application.exceptions;

public class EndPointException extends RuntimeException {

    private final static long serialVersionUID = 1L;

    public EndPointException(String aMessage) {
        super(aMessage);
    }

    public EndPointException(Throwable aCause) {
        super(aCause);
    }
}
