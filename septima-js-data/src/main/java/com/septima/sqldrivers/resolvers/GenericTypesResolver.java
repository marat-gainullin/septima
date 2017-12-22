package com.septima.sqldrivers.resolvers;

import com.septima.DataTypes;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author mg
 */
public class GenericTypesResolver implements TypesResolver {

    private static final Map<Integer, String> jdbcTypesToApplicationTypes = new LinkedHashMap<>(){{
        put(java.sql.Types.VARCHAR, DataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.CHAR, DataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.CLOB, DataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.DATALINK, DataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.LONGNVARCHAR, DataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.LONGVARCHAR, DataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NCHAR, DataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NCLOB, DataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NVARCHAR, DataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.ROWID, DataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.SQLXML, DataTypes.STRING_TYPE_NAME);
        put(java.sql.Types.BIGINT, DataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DECIMAL, DataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DOUBLE, DataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.FLOAT, DataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.INTEGER, DataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.NUMERIC, DataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.REAL, DataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.SMALLINT, DataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.TINYINT, DataTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DATE, DataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIME, DataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIMESTAMP, DataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, DataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIME_WITH_TIMEZONE, DataTypes.DATE_TYPE_NAME);
        put(java.sql.Types.BIT, DataTypes.BOOLEAN_TYPE_NAME);
        put(java.sql.Types.BOOLEAN, DataTypes.BOOLEAN_TYPE_NAME);
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
