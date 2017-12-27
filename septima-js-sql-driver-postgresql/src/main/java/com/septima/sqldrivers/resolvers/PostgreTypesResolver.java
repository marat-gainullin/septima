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
public class PostgreTypesResolver implements TypesResolver {

    private static final Map<String, GenericType> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>() {{
        put("character varying", GenericType.STRING);
        put("bpchar", GenericType.STRING);
        put("char", GenericType.STRING);
        put("character", GenericType.STRING);
        put("varchar", GenericType.STRING);
        put("name", GenericType.STRING);
        put("text", GenericType.STRING);
        put("int8", GenericType.LONG);
        put("bigint", GenericType.LONG);
        put("serial", GenericType.LONG);
        put("bigserial", GenericType.LONG);
        put("oid", GenericType.LONG);
        put("integer", GenericType.LONG);
        put("int", GenericType.LONG);
        put("int4", GenericType.LONG);
        put("smallint", GenericType.LONG);
        put("int2", GenericType.LONG);
        put("decimal", GenericType.DOUBLE);
        put("numeric", GenericType.DOUBLE);
        put("real", GenericType.DOUBLE);
        put("float4", GenericType.DOUBLE);
        put("double precision", GenericType.DOUBLE);
        put("float", GenericType.DOUBLE);
        put("float8", GenericType.DOUBLE);
        put("money", GenericType.DOUBLE);
        put("bool", GenericType.BOOLEAN);
        put("bit", GenericType.BOOLEAN);
        put("boolean", GenericType.BOOLEAN);
        put("date", GenericType.DATE);
        put("time", GenericType.DATE);
        put("timetz", GenericType.DATE);
        put("time with time zone", GenericType.DATE);
        put("time without time zone", GenericType.DATE);
        put("timestamptz", GenericType.DATE);
        put("timestamp with time zone", GenericType.DATE);
        put("timestamp without time zone", GenericType.DATE);
        put("timestamp", GenericType.DATE);
        put("bytea", null);
        // gis types
        put("geometry", GenericType.GEOMETRY);
        put("geography", GenericType.GEOMETRY);
        put("geometry", GenericType.GEOMETRY);
        put("point", GenericType.GEOMETRY);
        put("line", GenericType.GEOMETRY);
        put("lseg", GenericType.GEOMETRY);
        put("box", GenericType.GEOMETRY);
        put("path", GenericType.GEOMETRY);
        put("polygon", GenericType.GEOMETRY);
        put("circle", GenericType.GEOMETRY);
    }};
    private static final Set<String> jdbcTypesWithSize = new HashSet<>() {{
        add("decimal");
        add("numeric");
        add("bpchar");
        add("char");
        add("character");
        add("varchar");
        add("character varying");
    }};
    private static final Set<String> jdbcTypesWithScale = new HashSet<>() {{
        add("decimal");
        add("numeric");
    }};
    private static final Map<String, Integer> jdbcTypesMaxSize = new HashMap<>() {{
        put("bpchar", 10485760);
        put("char", 10485760);
        put("character", 10485760);
        put("varchar", 10485760);
        put("character varying", 10485760);
        put("name", 10485760); //????
        put("numeric", 1000);
        put("decimal", 1000);
    }};
    private static final Map<String, Integer> jdbcTypesDefaultSize = new HashMap<>() {{
        put("bpchar", 1);
        put("char", 1);
        put("character", 1);
        put("varchar", 200);
        put("character varying", 200);
    }};

    @Override
    public GenericType toGenericType(int aJdbcType, String aRdbmsTypeName) {
        return aRdbmsTypeName != null ? rdbmsTypes2ApplicationTypes.get(aRdbmsTypeName.toLowerCase()) : null;
    }

    @Override
    public Set<String> getSupportedTypes() {
        return Collections.unmodifiableSet(rdbmsTypes2ApplicationTypes.keySet());
    }

    @Override
    public boolean isSized(String aRdbmsTypeName) {
        return jdbcTypesWithSize.contains(aRdbmsTypeName.toLowerCase());
    }

    @Override
    public boolean isScaled(String aRdbmsTypeName) {
        return jdbcTypesWithScale.contains(aRdbmsTypeName.toLowerCase());
    }

    @Override
    public int resolveSize(String aRdbmsTypeName, int aSize) {
        if (aRdbmsTypeName != null) {
            // check on max size
            Integer maxSize = jdbcTypesMaxSize.getOrDefault(aRdbmsTypeName.toLowerCase(), Integer.MAX_VALUE);
            if (aSize > maxSize) {
                return maxSize;
            } else if (aSize <= 0 && jdbcTypesDefaultSize.containsKey(aRdbmsTypeName.toLowerCase())) {
                return jdbcTypesDefaultSize.get(aRdbmsTypeName.toLowerCase());
            } else {
                return aSize;
            }
        } else {
            return aSize;
        }
    }
}
