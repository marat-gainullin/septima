package com.septima.script;

/**
 * An exception to notify an attempt to set a publisher for an object more than once.
 * @author vv
 */
public class AlreadyPublishedException extends IllegalStateException {
    
    public AlreadyPublishedException() {
        super("API object has to be published only once!");
    }
}
