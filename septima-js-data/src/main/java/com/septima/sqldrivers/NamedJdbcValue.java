package com.septima.sqldrivers;

/**
 * @author mg
 */
public class NamedJdbcValue {

    private final String name;
    private final Object value;
    private final int jdbcType;
    private final String sqlTypeName;

    NamedJdbcValue(String aName, Object aValue, int aJdbcType, String aSqlTypeName) {
        name = aName;
        value = aValue;
        jdbcType = aJdbcType;
        sqlTypeName = aSqlTypeName;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public String getSqlTypeName() {
        return sqlTypeName;
    }

}
