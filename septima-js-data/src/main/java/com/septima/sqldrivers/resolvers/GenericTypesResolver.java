package com.septima.sqldrivers.resolvers;

import com.septima.application.ApplicationDataTypes;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author mg
 */
public class GenericTypesResolver implements TypesResolver {

    private static final Map<Integer, String> jdbcTypesToApplicationTypes = new LinkedHashMap<>(){{
        put(java.sql.Types.VARCHAR, ApplicationDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.CHAR, ApplicationDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.CLOB, ApplicationDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.DATALINK, ApplicationDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.LONGNVARCHAR, ApplicationDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.LONGVARCHAR, ApplicationDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NCHAR, ApplicationDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NCLOB, ApplicationDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NVARCHAR, ApplicationDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.ROWID, ApplicationDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.SQLXML, ApplicationDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.BIGINT, ApplicationDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DECIMAL, ApplicationDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DOUBLE, ApplicationDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.FLOAT, ApplicationDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.INTEGER, ApplicationDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.NUMERIC, ApplicationDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.REAL, ApplicationDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.SMALLINT, ApplicationDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.TINYINT, ApplicationDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DATE, ApplicationDataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIME, ApplicationDataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIMESTAMP, ApplicationDataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, ApplicationDataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIME_WITH_TIMEZONE, ApplicationDataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.BIT, ApplicationDataTypes.BOOLEAN_TYPE_NAME);
        put(java.sql.Types.BOOLEAN, ApplicationDataTypes.BOOLEAN_TYPE_NAME);
    }};

    @Override
    public String toApplicationType(int aJdbcType, String aRdbmsTypeName) {
        return jdbcTypesToApplicationTypes.get(aJdbcType);
    }

    @Override
    public Set<String> getSupportedTypes() {
        return null;
    }

    @Override
    public boolean isSized(String aRdbmsTypeName) {
        return false;
    }

    @Override
    public boolean isScaled(String aRdbmsTypeName) {
        return false;
    }

    @Override
    public int resolveSize(String aRdbmsTypeName, int aSize) {
        return aSize;
    }

}
