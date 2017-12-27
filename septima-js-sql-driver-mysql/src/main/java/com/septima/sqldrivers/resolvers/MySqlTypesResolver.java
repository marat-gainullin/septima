package com.septima.sqldrivers.resolvers;

import com.septima.GenericDataTypes;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author mg
 */
public class MySqlTypesResolver implements TypesResolver {

    private static final Map<String, String> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>(){{
        put("varchar", GenericDataTypes.STRING_TYPE_NAME);
        put("int", GenericDataTypes.NUMBER_TYPE_NAME);
        put("decimal", GenericDataTypes.NUMBER_TYPE_NAME);
        put("boolean", GenericDataTypes.BOOLEAN_TYPE_NAME);
        put("timestamp", GenericDataTypes.DATE_TYPE_NAME);
        put("datetime", GenericDataTypes.DATE_TYPE_NAME);
        put("geometry", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("tinyint", GenericDataTypes.NUMBER_TYPE_NAME);
        put("smallint", GenericDataTypes.NUMBER_TYPE_NAME);
        put("mediumint", GenericDataTypes.NUMBER_TYPE_NAME);
        put("integer", GenericDataTypes.NUMBER_TYPE_NAME);
        put("bigint", GenericDataTypes.NUMBER_TYPE_NAME);
        put("serial", GenericDataTypes.NUMBER_TYPE_NAME);
        put("float", GenericDataTypes.NUMBER_TYPE_NAME);
        put("real", GenericDataTypes.NUMBER_TYPE_NAME);
        put("double", GenericDataTypes.NUMBER_TYPE_NAME);
        put("double precision", GenericDataTypes.NUMBER_TYPE_NAME);
        put("dec", GenericDataTypes.NUMBER_TYPE_NAME);
        put("numeric", GenericDataTypes.NUMBER_TYPE_NAME);
        put("bool", GenericDataTypes.BOOLEAN_TYPE_NAME);
        put("bit", GenericDataTypes.BOOLEAN_TYPE_NAME);
        put("char", GenericDataTypes.STRING_TYPE_NAME);
        put("tinytext", GenericDataTypes.STRING_TYPE_NAME);
        put("long varchar", GenericDataTypes.STRING_TYPE_NAME);
        put("text", GenericDataTypes.STRING_TYPE_NAME);
        put("mediumtext", GenericDataTypes.STRING_TYPE_NAME);
        put("longtext", GenericDataTypes.STRING_TYPE_NAME);
        put("date", GenericDataTypes.DATE_TYPE_NAME);
        put("time", GenericDataTypes.DATE_TYPE_NAME);
        put("year", GenericDataTypes.DATE_TYPE_NAME);
        put("enum", GenericDataTypes.STRING_TYPE_NAME);
        put("set", GenericDataTypes.STRING_TYPE_NAME);
        put("binary", null);
        put("varbinary", null);
        put("tinyblob", null);
        put("blob", null);
        put("mediumblob", null);
        put("longblob", null);
        put("long varbinary", null);
        // gis types
        put("point", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("linestring", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("polygon", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("multipoint", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("multilinestring", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("multipolygon", GenericDataTypes.GEOMETRY_TYPE_NAME);
        put("geometrycollection", GenericDataTypes.GEOMETRY_TYPE_NAME);
    }};
    private static final Set<String> jdbcTypesWithSize = new HashSet<>(){{
        add("float");
        add("real");
        add("double");
        add("double precision");
        add("numeric");
        add("decimal");
        add("dec");
        add("char");
        add("varchar");
        add("binary");
        add("varbinary");
    }};
    private static final Set<String> jdbcTypesWithScale = new HashSet<>(){{
        add("float");
        add("real");
        add("double");
        add("double precision");
        add("numeric");
        add("decimal");
        add("dec");
    }};
    private static final Map<String, Integer> jdbcTypesMaxSize = new HashMap<>(){{
        put("char", 255);
        put("varchar", 65535);
        put("tinytext", 255);
        put("text", 65535);
        put("mediumtext", 16777215);

        put("longvarchar", 16777215);

        put("longtext", 2147483647);
        put("binary", 255);
        put("varbinary", 255);

        put("long varbinary", 16777215);
        put("tinyblob", 255);
        put("blob", 65535);
        put("mediumblob", 16777215);
        put("longblob", 2147483647);
    }};
    private static final Map<String, Integer> jdbcTypesDefaultSize = new HashMap<>(){{
        put("char", 1);
        put("varchar", 200);
        put("binary", 1);
        put("varbinary", 200);
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
