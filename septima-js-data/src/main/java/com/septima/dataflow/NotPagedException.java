package com.septima.dataflow;

/**
 * Exception class intended transform indicate an erroneous call transform method nextPage of
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
