package com.septima.sqldrivers.resolvers;

import com.septima.ApplicationTypes;

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
        put("character varying", ApplicationTypes.STRING_TYPE_NAME);
        put("decimal", ApplicationTypes.NUMBER_TYPE_NAME);
        put("boolean", ApplicationTypes.BOOLEAN_TYPE_NAME);
        put("timestamp", ApplicationTypes.DATE_TYPE_NAME);
        put("geometry", ApplicationTypes.GEOMETRY_TYPE_NAME);
        put("int8", ApplicationTypes.NUMBER_TYPE_NAME);
        put("bigint", ApplicationTypes.NUMBER_TYPE_NAME);
        put("bigserial", ApplicationTypes.NUMBER_TYPE_NAME);
        put("oid", ApplicationTypes.NUMBER_TYPE_NAME);
        put("numeric", ApplicationTypes.NUMBER_TYPE_NAME);
        put("integer", ApplicationTypes.NUMBER_TYPE_NAME);
        put("int", ApplicationTypes.NUMBER_TYPE_NAME);
        put("int4", ApplicationTypes.NUMBER_TYPE_NAME);
        put("serial", ApplicationTypes.NUMBER_TYPE_NAME);
        put("smallint", ApplicationTypes.NUMBER_TYPE_NAME);
        put("int2", ApplicationTypes.NUMBER_TYPE_NAME);
        put("real", ApplicationTypes.NUMBER_TYPE_NAME);
        put("float4", ApplicationTypes.NUMBER_TYPE_NAME);
        put("double precision", ApplicationTypes.NUMBER_TYPE_NAME);
        put("float", ApplicationTypes.NUMBER_TYPE_NAME);
        put("float8", ApplicationTypes.NUMBER_TYPE_NAME);
        put("money", ApplicationTypes.NUMBER_TYPE_NAME);
        put("bool", ApplicationTypes.BOOLEAN_TYPE_NAME);
        put("bit", ApplicationTypes.BOOLEAN_TYPE_NAME);
        put("bpchar", ApplicationTypes.STRING_TYPE_NAME);
        put("char", ApplicationTypes.STRING_TYPE_NAME);
        put("character", ApplicationTypes.STRING_TYPE_NAME);
        put("varchar", ApplicationTypes.STRING_TYPE_NAME);
        put("name", ApplicationTypes.STRING_TYPE_NAME);
        put("text", ApplicationTypes.STRING_TYPE_NAME);
        put("date", ApplicationTypes.DATE_TYPE_NAME);
        put("time", ApplicationTypes.DATE_TYPE_NAME);
        put("timetz", ApplicationTypes.DATE_TYPE_NAME);
        put("time with time zone", ApplicationTypes.DATE_TYPE_NAME);
        put("time without time zone", ApplicationTypes.DATE_TYPE_NAME);
        put("timestamptz", ApplicationTypes.DATE_TYPE_NAME);
        put("timestamp with time zone", ApplicationTypes.DATE_TYPE_NAME);
        put("timestamp without time zone", ApplicationTypes.DATE_TYPE_NAME);
        put("bytea", null);
        // gis types
        put("geography", ApplicationTypes.GEOMETRY_TYPE_NAME);
        put("geometry", ApplicationTypes.GEOMETRY_TYPE_NAME);
        put("point", ApplicationTypes.GEOMETRY_TYPE_NAME);
        put("line", ApplicationTypes.GEOMETRY_TYPE_NAME);
        put("lseg", ApplicationTypes.GEOMETRY_TYPE_NAME);
        put("box", ApplicationTypes.GEOMETRY_TYPE_NAME);
        put("path", ApplicationTypes.GEOMETRY_TYPE_NAME);
        put("polygon", ApplicationTypes.GEOMETRY_TYPE_NAME);
        put("circle", ApplicationTypes.GEOMETRY_TYPE_NAME);
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
