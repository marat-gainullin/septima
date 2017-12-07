package com.septima;

import com.septima.metadata.Field;
import com.septima.metadata.Parameter;


/**
 *
 * @author mg
 */
public interface EntitiesHost {

    public Field resolveField(String aEntityName, String aFieldName) throws Exception;
    
    public Parameter resolveParameter(String aEntityName, String aParamName) throws Exception;
}
