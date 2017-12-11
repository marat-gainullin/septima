package com.septima.dataflow;

/**
 * Exception, raised when any flow provider malfunction.
 *
 * @author mg
 * @see DataProvider
 */
public class DataProviderFailedException extends Exception {

    /**
     * Standard constructor of the exception
     *
     * @param aCause Exception, has caused this exception throwing.
     */
    public DataProviderFailedException(Exception aCause) {
        super(aCause);
    }

    /**
     * Standard constructor of the exception
     *
     * @param aMsg Description of the exception throwing cause.
     */
    public DataProviderFailedException(String aMsg) {
        super(aMsg);
    }
}
