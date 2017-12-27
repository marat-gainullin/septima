package com.septima.sqldrivers.resolvers;

import com.septima.GenericType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author kl
 */
public class Db2TypesResolver implements TypesResolver {

    private static final Map<String, GenericType> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>() {{
        put("VARCHAR", GenericType.STRING);
        put("LONG VARCHAR", GenericType.STRING);
        put("CHAR", GenericType.STRING);
        put("CHARACTER", GenericType.STRING);
        put("CHAR VARYING", GenericType.STRING);
        put("CHARACTER VARYING", GenericType.STRING);
        put("CLOB", GenericType.STRING);
        put("CHAR LARGE OBJECT", GenericType.STRING);
        put("CHARACTER LARGE OBJECT", GenericType.STRING);
        put("XML", GenericType.STRING); //?? OTHER  || SQLXML || BLOB
        put("INT", GenericType.LONG);
        put("SMALLINT", GenericType.LONG);
        put("INTEGER", GenericType.LONG);
        put("BIGINT", GenericType.LONG);
        put("NUMERIC", GenericType.DOUBLE);
        put("DECIMAL", GenericType.DOUBLE);
        put("DEC", GenericType.DOUBLE);
        put("NUM", GenericType.DOUBLE);
        put("FLOAT", GenericType.DOUBLE);
        put("REAL", GenericType.DOUBLE);
        put("DOUBLE", GenericType.DOUBLE);
        put("DOUBLE PRECISION", GenericType.DOUBLE);
        put("DECFLOAT", GenericType.DOUBLE);
        put("TIMESTAMP", GenericType.DATE);
        put("DATE", GenericType.DATE);
        put("TIME", GenericType.DATE);
        put("BLOB", null);
        put("BINARY LARGE OBJECT", null);
        put("LONG VARCHAR FOR BIT DATA", null);
    }};
    private static final Set<String> jdbcTypesWithSize = new HashSet<>() {{
        add("DECIMAL");
        add("DEC");
        add("NUMERIC");
        add("NUM");
        add("CHAR");
        add("CHARACTER");
        add("VARCHAR");
        add("CHAR VARYING");
        add("CHARACTER VARYING");
        add("CLOB");
        add("CHAR LARGE OBJECT");
        add("CHARACTER LARGE OBJECT");
        add("BLOB");
        add("BINARY LARGE OBJECT");
    }};
    private static final Set<String> jdbcTypesWithScale = new HashSet<>() {{
        add("DECIMAL");
        add("DEC");
        add("NUMERIC");
        add("NUM");
    }};
    private static final Map<String, Integer> jdbcTypesMaxSize = new HashMap<>() {{
        put("CHAR", 254);
        put("CHARACTER", 254);
        put("VARCHAR", 4000);
        put("CHAR VARYING", 4000);
        put("CHARACTER VARYING", 4000);
    }};
    private static final Map<String, Integer> jdbcTypesDefaultSize = new HashMap<>() {{
        put("CHAR", 1);
        put("CHARACTER", 1);
        put("VARCHAR", 200);
        put("CHAR VARYING", 200);
        put("CHARACTER VARYING", 200);
        put("CLOB", 2147483647);
        put("CHAR LARGE OBJECT", 2147483647);
        put("CHARACTER LARGE OBJECT", 2147483647);
        put("BLOB", 2147483647);
        put("BINARY LARGE OBJECT", 2147483647);
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
