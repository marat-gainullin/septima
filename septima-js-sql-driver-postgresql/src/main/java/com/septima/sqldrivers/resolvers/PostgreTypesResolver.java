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
public class PostgreTypesResolver implements TypesResolver {

    private static final Map<String, String> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>() {{
        put("character varying", DataTypes.STRING_TYPE_NAME);
        put("decimal", DataTypes.NUMBER_TYPE_NAME);
        put("boolean", DataTypes.BOOLEAN_TYPE_NAME);
        put("timestamp", DataTypes.DATE_TYPE_NAME);
        put("geometry", DataTypes.GEOMETRY_TYPE_NAME);
        put("int8", DataTypes.NUMBER_TYPE_NAME);
        put("bigint", DataTypes.NUMBER_TYPE_NAME);
        put("bigserial", DataTypes.NUMBER_TYPE_NAME);
        put("oid", DataTypes.NUMBER_TYPE_NAME);
        put("numeric", DataTypes.NUMBER_TYPE_NAME);
        put("integer", DataTypes.NUMBER_TYPE_NAME);
        put("int", DataTypes.NUMBER_TYPE_NAME);
        put("int4", DataTypes.NUMBER_TYPE_NAME);
        put("serial", DataTypes.NUMBER_TYPE_NAME);
        put("smallint", DataTypes.NUMBER_TYPE_NAME);
        put("int2", DataTypes.NUMBER_TYPE_NAME);
        put("real", DataTypes.NUMBER_TYPE_NAME);
        put("float4", DataTypes.NUMBER_TYPE_NAME);
        put("double precision", DataTypes.NUMBER_TYPE_NAME);
        put("float", DataTypes.NUMBER_TYPE_NAME);
        put("float8", DataTypes.NUMBER_TYPE_NAME);
        put("money", DataTypes.NUMBER_TYPE_NAME);
        put("bool", DataTypes.BOOLEAN_TYPE_NAME);
        put("bit", DataTypes.BOOLEAN_TYPE_NAME);
        put("bpchar", DataTypes.STRING_TYPE_NAME);
        put("char", DataTypes.STRING_TYPE_NAME);
        put("character", DataTypes.STRING_TYPE_NAME);
        put("varchar", DataTypes.STRING_TYPE_NAME);
        put("name", DataTypes.STRING_TYPE_NAME);
        put("text", DataTypes.STRING_TYPE_NAME);
        put("date", DataTypes.DATE_TYPE_NAME);
        put("time", DataTypes.DATE_TYPE_NAME);
        put("timetz", DataTypes.DATE_TYPE_NAME);
        put("time with time zone", DataTypes.DATE_TYPE_NAME);
        put("time without time zone", DataTypes.DATE_TYPE_NAME);
        put("timestamptz", DataTypes.DATE_TYPE_NAME);
        put("timestamp with time zone", DataTypes.DATE_TYPE_NAME);
        put("timestamp without time zone", DataTypes.DATE_TYPE_NAME);
        put("bytea", null);
        // gis types
        put("geography", DataTypes.GEOMETRY_TYPE_NAME);
        put("geometry", DataTypes.GEOMETRY_TYPE_NAME);
        put("point", DataTypes.GEOMETRY_TYPE_NAME);
        put("line", DataTypes.GEOMETRY_TYPE_NAME);
        put("lseg", DataTypes.GEOMETRY_TYPE_NAME);
        put("box", DataTypes.GEOMETRY_TYPE_NAME);
        put("path", DataTypes.GEOMETRY_TYPE_NAME);
        put("polygon", DataTypes.GEOMETRY_TYPE_NAME);
        put("circle", DataTypes.GEOMETRY_TYPE_NAME);
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
