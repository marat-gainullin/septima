package com.septima.sqldrivers.resolvers;

import com.septima.ApplicationTypes;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author mg
 */
public class GenericTypesResolver implements TypesResolver {

    private static final Map<Integer, String> jdbcTypesToApplicationTypes = new LinkedHashMap<>(){{
        put(java.sql.Types.VARCHAR, ApplicationTypes.STRING_TYPE_NAME);
        put(java.sql.Types.CHAR, ApplicationTypes.STRING_TYPE_NAME);
        put(java.sql.Types.CLOB, ApplicationTypes.STRING_TYPE_NAME);
        put(java.sql.Types.DATALINK, ApplicationTypes.STRING_TYPE_NAME);
        put(java.sql.Types.LONGNVARCHAR, ApplicationTypes.STRING_TYPE_NAME);
        put(java.sql.Types.LONGVARCHAR, ApplicationTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NCHAR, ApplicationTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NCLOB, ApplicationTypes.STRING_TYPE_NAME);
        put(java.sql.Types.NVARCHAR, ApplicationTypes.STRING_TYPE_NAME);
        put(java.sql.Types.ROWID, ApplicationTypes.STRING_TYPE_NAME);
        put(java.sql.Types.SQLXML, ApplicationTypes.STRING_TYPE_NAME);
        put(java.sql.Types.BIGINT, ApplicationTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DECIMAL, ApplicationTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DOUBLE, ApplicationTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.FLOAT, ApplicationTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.INTEGER, ApplicationTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.NUMERIC, ApplicationTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.REAL, ApplicationTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.SMALLINT, ApplicationTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.TINYINT, ApplicationTypes.NUMBER_TYPE_NAME);
        put(java.sql.Types.DATE, ApplicationTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIME, ApplicationTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIMESTAMP, ApplicationTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, ApplicationTypes.DATE_TYPE_NAME);
        put(java.sql.Types.TIME_WITH_TIMEZONE, ApplicationTypes.DATE_TYPE_NAME);
        put(java.sql.Types.BIT, ApplicationTypes.BOOLEAN_TYPE_NAME);
        put(java.sql.Types.BOOLEAN, ApplicationTypes.BOOLEAN_TYPE_NAME);
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
