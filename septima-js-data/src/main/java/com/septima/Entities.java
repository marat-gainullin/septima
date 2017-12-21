package com.septima;

import com.septima.metadata.Field;
import com.septima.queries.SqlEntity;

import java.nio.file.Path;
import java.sql.SQLException;


/**
 * @author mg
 */
public interface Entities {

    Field resolveField(String aEntityName, String aFieldName) throws SQLException;

    Parameter resolveParameter(String aEntityName, String aParamName);

    SqlEntity loadEntity(String aEntityName);

    Path getApplicationPath();
}
