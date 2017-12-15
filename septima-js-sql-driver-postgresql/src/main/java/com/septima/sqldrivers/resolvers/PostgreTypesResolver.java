package com.septima.sqldrivers.resolvers;

import com.septima.application.ApplicationDataTypes;

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
        put("character varying", ApplicationDataTypes.STRING_TYPE_NAME);
        put("decimal", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("boolean", ApplicationDataTypes.BOOLEAN_TYPE_NAME);
        put("timestamp", ApplicationDataTypes.DATE_TYPE_NAME);
        put("geometry", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("int8", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("bigint", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("bigserial", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("oid", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("numeric", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("integer", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("int", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("int4", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("serial", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("smallint", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("int2", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("real", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("float4", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("double precision", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("float", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("float8", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("money", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("bool", ApplicationDataTypes.BOOLEAN_TYPE_NAME);
        put("bit", ApplicationDataTypes.BOOLEAN_TYPE_NAME);
        put("bpchar", ApplicationDataTypes.STRING_TYPE_NAME);
        put("char", ApplicationDataTypes.STRING_TYPE_NAME);
        put("character", ApplicationDataTypes.STRING_TYPE_NAME);
        put("varchar", ApplicationDataTypes.STRING_TYPE_NAME);
        put("name", ApplicationDataTypes.STRING_TYPE_NAME);
        put("text", ApplicationDataTypes.STRING_TYPE_NAME);
        put("date", ApplicationDataTypes.DATE_TYPE_NAME);
        put("time", ApplicationDataTypes.DATE_TYPE_NAME);
        put("timetz", ApplicationDataTypes.DATE_TYPE_NAME);
        put("time with time zone", ApplicationDataTypes.DATE_TYPE_NAME);
        put("time without time zone", ApplicationDataTypes.DATE_TYPE_NAME);
        put("timestamptz", ApplicationDataTypes.DATE_TYPE_NAME);
        put("timestamp with time zone", ApplicationDataTypes.DATE_TYPE_NAME);
        put("timestamp without time zone", ApplicationDataTypes.DATE_TYPE_NAME);
        put("bytea", null);
        // gis types
        put("geography", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("geometry", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("point", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("line", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("lseg", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("box", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("path", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("polygon", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("circle", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
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
