package com.septima.dataflow;

/**
 * Exception class intended to indicate an erroneous call to method nextPage of
 * the DataProvider interface, when flow provider is not paged.
 *
 * @author mg
 */
public class NotPagedException extends Exception {

    /**
     * {@inheritDoc}
     */
    public NotPagedException(String aMsg) {
        super(aMsg);
    }
}
