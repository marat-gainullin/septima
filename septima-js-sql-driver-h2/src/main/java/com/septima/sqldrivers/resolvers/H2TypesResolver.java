package com.septima.sqldrivers.resolvers;

import com.septima.ApplicationTypes;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author vv
 */
public class H2TypesResolver implements TypesResolver {

    private static final Map<String, String> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>() {{
        put("VARCHAR", ApplicationTypes.STRING_TYPE_NAME);
        put("NUMERIC", ApplicationTypes.NUMBER_TYPE_NAME);
        put("DECIMAL", ApplicationTypes.NUMBER_TYPE_NAME);
        put("BOOLEAN", ApplicationTypes.BOOLEAN_TYPE_NAME);
        put("TIMESTAMP", ApplicationTypes.DATE_TYPE_NAME);
        put("TINYINT", ApplicationTypes.NUMBER_TYPE_NAME);
        put("BIGINT", ApplicationTypes.NUMBER_TYPE_NAME);
        put("IDENTITY", ApplicationTypes.NUMBER_TYPE_NAME);
        put("INTEGER", ApplicationTypes.NUMBER_TYPE_NAME);
        put("SMALLINT", ApplicationTypes.NUMBER_TYPE_NAME);
        put("FLOAT", ApplicationTypes.NUMBER_TYPE_NAME);
        put("REAL", ApplicationTypes.NUMBER_TYPE_NAME);
        put("DOUBLE", ApplicationTypes.NUMBER_TYPE_NAME);
        put("LONGVARCHAR", ApplicationTypes.STRING_TYPE_NAME);
        put("CHAR", ApplicationTypes.STRING_TYPE_NAME);
        put("VARCHAR_IGNORECASE", ApplicationTypes.STRING_TYPE_NAME);
        put("DATE", ApplicationTypes.DATE_TYPE_NAME);
        put("TIME", ApplicationTypes.DATE_TYPE_NAME);
        put("CLOB", ApplicationTypes.STRING_TYPE_NAME);
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
