package com.septima;

import com.septima.metadata.Field;

import java.sql.SQLException;


/**
 *
 * @author mg
 */
public interface EntitiesHost {

    Field resolveField(String aEntityName, String aFieldName) throws SQLException;
    
    Parameter resolveParameter(String aEntityName, String aParamName);
}
