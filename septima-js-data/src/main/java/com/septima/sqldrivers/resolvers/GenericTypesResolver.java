package com.septima.sqldrivers.resolvers;

import com.septima.GenericDataTypes;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author mg
 */
public class GenericTypesResolver implements TypesResolver {

    private static final Map<Integer, String> jdbcTypesToApplicationTypes = new LinkedHashMap<>(){{
        put(java.sql.Types.VARCHAR, GenericDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.CHAR, GenericDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.CLOB, GenericDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.DATALINK, GenericDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.LONGNVARCHAR, GenericDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.LONGVARCHAR, GenericDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NCHAR, GenericDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NCLOB, GenericDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NVARCHAR, GenericDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.ROWID, GenericDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.SQLXML, GenericDataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.BIGINT, GenericDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DECIMAL, GenericDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DOUBLE, GenericDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.FLOAT, GenericDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.INTEGER, GenericDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.NUMERIC, GenericDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.REAL, GenericDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.SMALLINT, GenericDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.TINYINT, GenericDataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DATE, GenericDataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIME, GenericDataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIMESTAMP, GenericDataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, GenericDataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIME_WITH_TIMEZONE, GenericDataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.BIT, GenericDataTypes.BOOLEAN_TYPE_NAME);
        put(java.sql.Types.BOOLEAN, GenericDataTypes.BOOLEAN_TYPE_NAME);
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
