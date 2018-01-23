package com.septima.application.exceptions;

public class InvalidRequestException extends EndPointException {

    private final static long serialVersionUID = 1L;

    public InvalidRequestException(String aMessage) {
        super(aMessage);
    }
}
