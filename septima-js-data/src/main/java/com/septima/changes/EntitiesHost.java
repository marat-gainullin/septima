package com.septima.client.changes;

import com.septima.client.metadata.Field;
import com.septima.client.metadata.Parameter;


/**
 *
 * @author mg
 */
public interface EntitiesHost {

    public Field resolveField(String aEntityName, String aFieldName) throws Exception;
    
    public Parameter resolveParameter(String aEntityName, String aParamName) throws Exception;
}
