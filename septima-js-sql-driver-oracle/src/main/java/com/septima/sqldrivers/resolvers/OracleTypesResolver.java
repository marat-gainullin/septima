package com.septima.sqldrivers.resolvers;

import com.septima.GenericType;

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

    private static final Map<String, GenericType> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>() {{
        put("VARCHAR2", GenericType.STRING);
        put("NVARCHAR2", GenericType.STRING);
        put("NCHAR", GenericType.STRING);
        put("LONG RAW", GenericType.STRING);
        put("RAW", GenericType.STRING);
        put("LONG", GenericType.STRING);
        put("CHAR", GenericType.STRING);
        put("CLOB", GenericType.STRING);
        put("NCLOB", GenericType.STRING);
        put("DECIMAL", GenericType.DOUBLE);
        put("DOUBLE", GenericType.DOUBLE);
        put("INTEGER", GenericType.LONG);
        put("FLOAT", GenericType.DOUBLE);
        put("REAL", GenericType.DOUBLE);
        put("DATE", GenericType.DATE);
        put("TIMESTAMP", GenericType.DATE);
        put("TIMESTAMP(6)", GenericType.DATE);
        put("TIMESTAMP WITH TIME ZONE", GenericType.DATE);
        put("TIMESTAMP WITH LOCAL TIME ZONE", GenericType.DATE);
        put("TIMESTAMP(6) WITH TIME ZONE", GenericType.DATE);
        put("TIMESTAMP(6) WITH LOCAL TIME ZONE", GenericType.DATE);
        put("MDSYS.SDO_GEOMETRY", GenericType.GEOMETRY);
        put("GEOMETRY", GenericType.GEOMETRY);
        put("CURVE", GenericType.GEOMETRY);
        put("POLYGON", GenericType.GEOMETRY);
        put("LINESTRING", GenericType.GEOMETRY);
        put("POINT", GenericType.GEOMETRY);
        put("SURFACE", GenericType.GEOMETRY);
        put("SDO_GEOMETRY", GenericType.GEOMETRY);
        put("BLOB", null);
    }};
    private static final Set<String> jdbcTypesWithSize = new HashSet<>() {{
        add("FLOAT");
        add("CHAR");
        add("VARCHAR2");
        add("NCHAR");
        add("NVARCHAR2");
        add("DOUBLE");
        add("DECIMAL");
        add("RAW");
    }};
    private static final Set<String> jdbcTypesWithScale = new HashSet<>() {{
        add("DECIMAL");
        add("DOUBLE");
    }};
    private static final Map<String, Integer> jdbcTypesMaxSize = new HashMap<>() {{
        put("CHAR", 255);
        put("VARCHAR2", 4000);
        put("NCHAR", 255);
        put("NVARCHAR2", 4000);
        put("DOUBLE", 38);
        put("DECIMAL", 38);
        put("RAW", 2000);
    }};
    private static final Map<String, Integer> jdbcTypesDefaultSize = new HashMap<>() {{
        put("CHAR", 1);
        put("VARCHAR2", 200);
        put("NCHAR2", 1);
        put("NVARCHAR2", 200);
        put("RAW", 1);
        put("DOUBLE", 38);
    }};

    @Override
    public GenericType toGenericType(int aJdbcType, String aRdbmsTypeName) {
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
