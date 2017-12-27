package com.septima.sqldrivers.resolvers;

import com.septima.GenericDataTypes;

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
        put("character varying", GenericDataTypes.STRING_TYPE_NAME);
        put("decimal", GenericDataTypes.NUMBER_TYPE_NAME);
        put("boolean", GenericDataTypes.BOOLEAN_TYPE_NAME);
        put("timestamp", GenericDataTypes.DATE_TYPE_NAME);
        put("geometry", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("int8", GenericDataTypes.NUMBER_TYPE_NAME);
        put("bigint", GenericDataTypes.NUMBER_TYPE_NAME);
        put("bigserial", GenericDataTypes.NUMBER_TYPE_NAME);
        put("oid", GenericDataTypes.NUMBER_TYPE_NAME);
        put("numeric", GenericDataTypes.NUMBER_TYPE_NAME);
        put("integer", GenericDataTypes.NUMBER_TYPE_NAME);
        put("int", GenericDataTypes.NUMBER_TYPE_NAME);
        put("int4", GenericDataTypes.NUMBER_TYPE_NAME);
        put("serial", GenericDataTypes.NUMBER_TYPE_NAME);
        put("smallint", GenericDataTypes.NUMBER_TYPE_NAME);
        put("int2", GenericDataTypes.NUMBER_TYPE_NAME);
        put("real", GenericDataTypes.NUMBER_TYPE_NAME);
        put("float4", GenericDataTypes.NUMBER_TYPE_NAME);
        put("double precision", GenericDataTypes.NUMBER_TYPE_NAME);
        put("float", GenericDataTypes.NUMBER_TYPE_NAME);
        put("float8", GenericDataTypes.NUMBER_TYPE_NAME);
        put("money", GenericDataTypes.NUMBER_TYPE_NAME);
        put("bool", GenericDataTypes.BOOLEAN_TYPE_NAME);
        put("bit", GenericDataTypes.BOOLEAN_TYPE_NAME);
        put("bpchar", GenericDataTypes.STRING_TYPE_NAME);
        put("char", GenericDataTypes.STRING_TYPE_NAME);
        put("character", GenericDataTypes.STRING_TYPE_NAME);
        put("varchar", GenericDataTypes.STRING_TYPE_NAME);
        put("name", GenericDataTypes.STRING_TYPE_NAME);
        put("text", GenericDataTypes.STRING_TYPE_NAME);
        put("date", GenericDataTypes.DATE_TYPE_NAME);
        put("time", GenericDataTypes.DATE_TYPE_NAME);
        put("timetz", GenericDataTypes.DATE_TYPE_NAME);
        put("time with time zone", GenericDataTypes.DATE_TYPE_NAME);
        put("time without time zone", GenericDataTypes.DATE_TYPE_NAME);
        put("timestamptz", GenericDataTypes.DATE_TYPE_NAME);
        put("timestamp with time zone", GenericDataTypes.DATE_TYPE_NAME);
        put("timestamp without time zone", GenericDataTypes.DATE_TYPE_NAME);
        put("bytea", null);
        // gis types
        put("geography", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("geometry", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("point", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("line", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("lseg", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("box", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("path", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("polygon", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("circle", GenericDataTypes.GEOMETRY_TYPE_NAME);
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
