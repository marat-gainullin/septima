/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.dataflow;

/**
 * Exception class intended to indicate an erronous call to method nextPage of
 * the DataProvider interface, when flow provider is not paged.
 *
 * @author mg
 */
public class DataProviderNotPagedException extends DataProviderFailedException {

    /**
     * {@inheritDoc}
     */
    public DataProviderNotPagedException(Exception aCause) {
        super(aCause);
    }

    /**
     * {@inheritDoc}
     */
    public DataProviderNotPagedException(String aMsg) {
        super(aMsg);
    }
}
