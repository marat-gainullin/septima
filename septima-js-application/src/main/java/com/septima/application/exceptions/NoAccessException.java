package com.septima.application.exceptions;

public class NoAccessException extends EndPointException {

    private final static long serialVersionUID = 1L;

    public NoAccessException(String aMessage) {
        super(aMessage);
    }
}
