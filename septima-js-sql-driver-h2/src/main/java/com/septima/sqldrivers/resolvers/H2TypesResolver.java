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
public class H2TypesResolver implements TypesResolver {

    private static final Map<String, GenericType> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>() {{
        put("VARCHAR", GenericType.STRING);
        put("LONGVARCHAR", GenericType.STRING);
        put("CHAR", GenericType.STRING);
        put("VARCHAR_IGNORECASE", GenericType.STRING);
        put("CLOB", GenericType.STRING);
        put("TINYINT", GenericType.LONG);
        put("BIGINT", GenericType.LONG);
        put("IDENTITY", GenericType.LONG);
        put("INTEGER", GenericType.LONG);
        put("SMALLINT", GenericType.LONG);
        put("NUMERIC", GenericType.DOUBLE);
        put("DECIMAL", GenericType.DOUBLE);
        put("FLOAT", GenericType.DOUBLE);
        put("REAL", GenericType.DOUBLE);
        put("DOUBLE", GenericType.DOUBLE);
        put("BOOLEAN", GenericType.BOOLEAN);
        put("TIMESTAMP", GenericType.DATE);
        put("DATE", GenericType.DATE);
        put("TIME", GenericType.DATE);
        put("GEOMETRY", GenericType.GEOMETRY);
        put("LONGVARBINARY", null);
        put("VARBINARY", null);
        put("BINARY", null);
        put("UUID", null);
        put("OTHER", null);
        put("ARRAY", null);
        put("BLOB", null);
    }};
    private static final Set<String> jdbcTypesWithSize = new HashSet<>() {{
        add("LONGVARBINARY");
        add("VARBINARY");
        add("BINARY");
        add("UUID");
        add("LONGVARCHAR");
        add("CHAR");
        add("VARCHAR");
        add("VARCHAR_IGNORECASE");
        add("OTHER");
        add("BLOB");
        add("CLOB");
    }};
    private static final Set<String> jdbcTypesWithScale = new HashSet<>() {{
        add("NUMERIC");
        add("DECIMAL");
    }};
    private static final Map<String, Integer> jdbcTypesMaxSize = new HashMap<>() {{
        put("CHAR", 2147483647);
        put("VARCHAR", 2147483647);
        put("VARCHAR_IGNORECASE", 2147483647);
        put("BINARY", 2147483647);
        put("UUID", 2147483647);
        put("VARBINARY", 2147483647);
    }};
    private static final Map<String, Integer> jdbcTypesDefaultSize = new HashMap<>() {{
        put("CHAR", 1);
        put("VARCHAR", 200);
        put("VARCHAR_IGNORECASE", 200);
        put("BINARY", 1);
        put("UUID", 1);
        put("VARBINARY", 200);
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
            if (maxSize < aSize) {
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
