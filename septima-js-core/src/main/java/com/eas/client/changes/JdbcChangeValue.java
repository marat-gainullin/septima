package com.eas.client.changes;

/**
 *
 * @author mg
 */
public class JdbcChangeValue extends ChangeValue {

    public int jdbcType;
    public String sqlTypeName;
    
    public JdbcChangeValue(String aName, Object aValue, int aJdbcType, String aSqlTypeName) {
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
