package com.septima;

import com.septima.metadata.Field;
import com.septima.entities.SqlEntity;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Set;


/**
 * @author mg
 */
public interface Entities {

    Field resolveField(String aEntityName, String aFieldName) throws SQLException;

    Parameter resolveParameter(String aEntityName, String aParamName);

    SqlEntity loadEntity(String aEntityName);

    SqlEntity loadEntity(String aEntityName, Set<String> illegalReferences);

    Path getApplicationPath();

    String getDefaultDataSource();
}
