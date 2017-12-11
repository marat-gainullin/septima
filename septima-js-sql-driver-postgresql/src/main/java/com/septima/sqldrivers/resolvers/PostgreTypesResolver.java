package com.septima.sqldrivers.resolvers;

import com.septima.Constants;

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

    private static final Map<String, String> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>() {{
        put("character varying", Constants.STRING_TYPE_NAME);
        put("decimal", Constants.NUMBER_TYPE_NAME);
        put("boolean", Constants.BOOLEAN_TYPE_NAME);
        put("timestamp", Constants.DATE_TYPE_NAME);
        put("geometry", Constants.GEOMETRY_TYPE_NAME);
        put("int8", Constants.NUMBER_TYPE_NAME);
        put("bigint", Constants.NUMBER_TYPE_NAME);
        put("bigserial", Constants.NUMBER_TYPE_NAME);
        put("oid", Constants.NUMBER_TYPE_NAME);
        put("numeric", Constants.NUMBER_TYPE_NAME);
        put("integer", Constants.NUMBER_TYPE_NAME);
        put("int", Constants.NUMBER_TYPE_NAME);
        put("int4", Constants.NUMBER_TYPE_NAME);
        put("serial", Constants.NUMBER_TYPE_NAME);
        put("smallint", Constants.NUMBER_TYPE_NAME);
        put("int2", Constants.NUMBER_TYPE_NAME);
        put("real", Constants.NUMBER_TYPE_NAME);
        put("float4", Constants.NUMBER_TYPE_NAME);
        put("double precision", Constants.NUMBER_TYPE_NAME);
        put("float", Constants.NUMBER_TYPE_NAME);
        put("float8", Constants.NUMBER_TYPE_NAME);
        put("money", Constants.NUMBER_TYPE_NAME);
        put("bool", Constants.BOOLEAN_TYPE_NAME);
        put("bit", Constants.BOOLEAN_TYPE_NAME);
        put("bpchar", Constants.STRING_TYPE_NAME);
        put("char", Constants.STRING_TYPE_NAME);
        put("character", Constants.STRING_TYPE_NAME);
        put("varchar", Constants.STRING_TYPE_NAME);
        put("name", Constants.STRING_TYPE_NAME);
        put("text", Constants.STRING_TYPE_NAME);
        put("date", Constants.DATE_TYPE_NAME);
        put("time", Constants.DATE_TYPE_NAME);
        put("timetz", Constants.DATE_TYPE_NAME);
        put("time with time zone", Constants.DATE_TYPE_NAME);
        put("time without time zone", Constants.DATE_TYPE_NAME);
        put("timestamptz", Constants.DATE_TYPE_NAME);
        put("timestamp with time zone", Constants.DATE_TYPE_NAME);
        put("timestamp without time zone", Constants.DATE_TYPE_NAME);
        put("bytea", null);
        // gis types
        put("geography", Constants.GEOMETRY_TYPE_NAME);
        put("geometry", Constants.GEOMETRY_TYPE_NAME);
        put("point", Constants.GEOMETRY_TYPE_NAME);
        put("line", Constants.GEOMETRY_TYPE_NAME);
        put("lseg", Constants.GEOMETRY_TYPE_NAME);
        put("box", Constants.GEOMETRY_TYPE_NAME);
        put("path", Constants.GEOMETRY_TYPE_NAME);
        put("polygon", Constants.GEOMETRY_TYPE_NAME);
        put("circle", Constants.GEOMETRY_TYPE_NAME);
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
    public String toApplicationType(int aJdbcType, String aRdbmsTypeName) {
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
