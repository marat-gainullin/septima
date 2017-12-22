package com.septima.sqldrivers.resolvers;

import com.septima.DataTypes;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author mg
 */
public class OracleTypesResolver implements TypesResolver {

    private static final Map<String, String> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>() {{
        put("VARCHAR2", DataTypes.STRING_TYPE_NAME);
        put("DECIMAL", DataTypes.NUMBER_TYPE_NAME);
        put("NUMBER", DataTypes.NUMBER_TYPE_NAME);
        put("TIMESTAMP", DataTypes.DATE_TYPE_NAME);
        put("MDSYS.SDO_GEOMETRY", DataTypes.GEOMETRY_TYPE_NAME);
        put("INTEGER", DataTypes.NUMBER_TYPE_NAME);
        put("FLOAT", DataTypes.NUMBER_TYPE_NAME);
        put("REAL", DataTypes.NUMBER_TYPE_NAME);
        put("DATE", DataTypes.DATE_TYPE_NAME);
        put("NVARCHAR2", DataTypes.STRING_TYPE_NAME);
        put("NCHAR", DataTypes.STRING_TYPE_NAME);
        put("TIMESTAMP(6)", DataTypes.DATE_TYPE_NAME);
        put("TIMESTAMP WITH TIME ZONE", DataTypes.DATE_TYPE_NAME);
        put("TIMESTAMP WITH LOCAL TIME ZONE", DataTypes.DATE_TYPE_NAME);
        put("TIMESTAMP(6) WITH TIME ZONE", DataTypes.DATE_TYPE_NAME);
        put("TIMESTAMP(6) WITH LOCAL TIME ZONE", DataTypes.DATE_TYPE_NAME);
        put("LONG RAW", DataTypes.STRING_TYPE_NAME);
        put("RAW", DataTypes.STRING_TYPE_NAME);
        put("LONG", DataTypes.STRING_TYPE_NAME);
        put("CHAR", DataTypes.STRING_TYPE_NAME);
        put("CLOB", DataTypes.STRING_TYPE_NAME);
        put("NCLOB", DataTypes.STRING_TYPE_NAME);
        put("GEOMETRY", DataTypes.GEOMETRY_TYPE_NAME);
        put("CURVE", DataTypes.GEOMETRY_TYPE_NAME);
        put("POLYGON", DataTypes.GEOMETRY_TYPE_NAME);
        put("LINESTRING", DataTypes.GEOMETRY_TYPE_NAME);
        put("POINT", DataTypes.GEOMETRY_TYPE_NAME);
        put("SURFACE", DataTypes.GEOMETRY_TYPE_NAME);
        put("SDO_GEOMETRY", DataTypes.GEOMETRY_TYPE_NAME);
        put("BLOB", null);
    }};
    private static final Set<String> jdbcTypesWithSize = new HashSet<>() {{
        add("FLOAT");
        add("CHAR");
        add("VARCHAR2");
        add("NCHAR");
        add("NVARCHAR2");
        add("NUMBER");
        add("DECIMAL");
        add("RAW");
    }};
    private static final Set<String> jdbcTypesWithScale = new HashSet<>() {{
        add("DECIMAL");
        add("NUMBER");
    }};
    private static final Map<String, Integer> jdbcTypesMaxSize = new HashMap<>() {{
        put("CHAR", 255);
        put("VARCHAR2", 4000);
        put("NCHAR", 255);
        put("NVARCHAR2", 4000);
        put("NUMBER", 38);
        put("DECIMAL", 38);
        put("RAW", 2000);
    }};
    private static final Map<String, Integer> jdbcTypesDefaultSize = new HashMap<>() {{
        put("CHAR", 1);
        put("VARCHAR2", 200);
        put("NCHAR2", 1);
        put("NVARCHAR2", 200);
        put("RAW", 1);
        put("NUMBER", 38);
    }};

    @Override
    public String toApplicationType(int aJdbcType, String aRdbmsTypeName) {
        return aRdbmsTypeName != null ? rdbmsTypes2ApplicationTypes.get(aRdbmsTypeName.toUpperCase()) : null;
    }

    @Override
    public Set<String> getSupportedTypes() {
        return Collections.unmodifiableSet(rdbmsTypes2ApplicationTypes.keySet());
    }

    @Override
    public boolean isSized(String aRdbmsTypeName) {
        return jdbcTypesWithSize.contains(aRdbmsTypeName.toUpperCase());
    }

    @Override
    public boolean isScaled(String aRdbmsTypeName) {
        return jdbcTypesWithScale.contains(aRdbmsTypeName.toUpperCase());
    }

    @Override
    public int resolveSize(String aRdbmsTypeName, int aSize) {
        if (aRdbmsTypeName != null) {
            // check on max size
            Integer maxSize = jdbcTypesMaxSize.getOrDefault(aRdbmsTypeName.toUpperCase(), Integer.MAX_VALUE);
            if (aSize > maxSize) {
                return maxSize;
            } else if (aSize <= 0 && jdbcTypesDefaultSize.containsKey(aRdbmsTypeName.toUpperCase())) {
                return jdbcTypesDefaultSize.get(aRdbmsTypeName.toUpperCase());
            } else {
                return aSize;
            }
        } else {
            return aSize;
        }
    }
}
