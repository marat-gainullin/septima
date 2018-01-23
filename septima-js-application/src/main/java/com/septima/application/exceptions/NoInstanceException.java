package com.septima.application.exceptions;

public class NoInstanceException extends EndPointException {

    private final static long serialVersionUID = 1L;

    public NoInstanceException(String aCollectionRef, String aKeyName, String aInstanceKey) {
        super("Collection '" + aCollectionRef + "' doesn't contain an instance with a key: " + aKeyName + " = " + aInstanceKey);
    }

}
