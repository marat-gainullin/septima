package com.septima.jdbc;

import com.septima.NamedValue;

/**
 *
 * @author mg
 */
public class NamedJdbcValue extends NamedValue {

    private final int jdbcType;
    private final String sqlTypeName;
    
    public NamedJdbcValue(String aName, Object aValue, int aJdbcType, String aSqlTypeName) {
        super(aName, aValue);
        jdbcType = aJdbcType;
        sqlTypeName = aSqlTypeName;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public String getSqlTypeName() {
        return sqlTypeName;
    }
    
}
