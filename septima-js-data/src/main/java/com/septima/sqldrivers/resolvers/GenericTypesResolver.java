package com.septima.sqldrivers.resolvers;

import com.septima.Constants;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author mg
 */
public class GenericTypesResolver implements TypesResolver {

    private static final Map<Integer, String> jdbcTypesToApplicationTypes = new LinkedHashMap<>(){{
        put(java.sql.Types.VARCHAR, Constants.STRING_TYPE_NAME);
        put(java.sql.Types.CHAR, Constants.STRING_TYPE_NAME);
        put(java.sql.Types.CLOB, Constants.STRING_TYPE_NAME);
        put(java.sql.Types.DATALINK, Constants.STRING_TYPE_NAME);
        put(java.sql.Types.LONGNVARCHAR, Constants.STRING_TYPE_NAME);
        put(java.sql.Types.LONGVARCHAR, Constants.STRING_TYPE_NAME);
        put(java.sql.Types.NCHAR, Constants.STRING_TYPE_NAME);
        put(java.sql.Types.NCLOB, Constants.STRING_TYPE_NAME);
        put(java.sql.Types.NVARCHAR, Constants.STRING_TYPE_NAME);
        put(java.sql.Types.ROWID, Constants.STRING_TYPE_NAME);
        put(java.sql.Types.SQLXML, Constants.STRING_TYPE_NAME);
        put(java.sql.Types.BIGINT, Constants.NUMBER_TYPE_NAME);
        put(java.sql.Types.DECIMAL, Constants.NUMBER_TYPE_NAME);
        put(java.sql.Types.DOUBLE, Constants.NUMBER_TYPE_NAME);
        put(java.sql.Types.FLOAT, Constants.NUMBER_TYPE_NAME);
        put(java.sql.Types.INTEGER, Constants.NUMBER_TYPE_NAME);
        put(java.sql.Types.NUMERIC, Constants.NUMBER_TYPE_NAME);
        put(java.sql.Types.REAL, Constants.NUMBER_TYPE_NAME);
        put(java.sql.Types.SMALLINT, Constants.NUMBER_TYPE_NAME);
        put(java.sql.Types.TINYINT, Constants.NUMBER_TYPE_NAME);
        put(java.sql.Types.DATE, Constants.DATE_TYPE_NAME);
        put(java.sql.Types.TIME, Constants.DATE_TYPE_NAME);
        put(java.sql.Types.TIMESTAMP, Constants.DATE_TYPE_NAME);
        put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, Constants.DATE_TYPE_NAME);
        put(java.sql.Types.TIME_WITH_TIMEZONE, Constants.DATE_TYPE_NAME);
        put(java.sql.Types.BIT, Constants.BOOLEAN_TYPE_NAME);
        put(java.sql.Types.BOOLEAN, Constants.BOOLEAN_TYPE_NAME);
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
