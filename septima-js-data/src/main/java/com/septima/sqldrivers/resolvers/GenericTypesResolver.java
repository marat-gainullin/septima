package com.septima.sqldrivers.resolvers;

import com.septima.GenericType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author mg
 */
public class GenericTypesResolver implements TypesResolver {

    private static final Map<Integer, GenericType> jdbcTypesToApplicationTypes = new LinkedHashMap<>(){{
        put(java.sql.Types.VARCHAR, GenericType.STRING);
        put(java.sql.Types.CHAR, GenericType.STRING);
        put(java.sql.Types.CLOB, GenericType.STRING);
        put(java.sql.Types.DATALINK, GenericType.STRING);
        put(java.sql.Types.LONGNVARCHAR, GenericType.STRING);
        put(java.sql.Types.LONGVARCHAR, GenericType.STRING);
        put(java.sql.Types.NCHAR, GenericType.STRING);
        put(java.sql.Types.NCLOB, GenericType.STRING);
        put(java.sql.Types.NVARCHAR, GenericType.STRING);
        put(java.sql.Types.ROWID, GenericType.STRING);
        put(java.sql.Types.SQLXML, GenericType.STRING);
        put(java.sql.Types.BIGINT, GenericType.LONG);
        put(java.sql.Types.DECIMAL, GenericType.DOUBLE);
        put(java.sql.Types.DOUBLE, GenericType.DOUBLE);
        put(java.sql.Types.FLOAT, GenericType.DOUBLE);
        put(java.sql.Types.INTEGER, GenericType.LONG);
        put(java.sql.Types.NUMERIC, GenericType.DOUBLE);
        put(java.sql.Types.REAL, GenericType.DOUBLE);
        put(java.sql.Types.SMALLINT, GenericType.LONG);
        put(java.sql.Types.TINYINT, GenericType.LONG);
        put(java.sql.Types.DATE, GenericType.DATE);
        put(java.sql.Types.TIME, GenericType.DATE);
        put(java.sql.Types.TIMESTAMP, GenericType.DATE);
        put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, GenericType.DATE);
        put(java.sql.Types.TIME_WITH_TIMEZONE, GenericType.DATE);
        put(java.sql.Types.BIT, GenericType.BOOLEAN);
        put(java.sql.Types.BOOLEAN, GenericType.BOOLEAN);
    }};

    @Override
    public GenericType toGenericType(int aJdbcType, String aRdbmsTypeName) {
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
