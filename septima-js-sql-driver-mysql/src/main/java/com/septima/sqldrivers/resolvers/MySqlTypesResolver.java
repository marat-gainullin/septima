package com.septima.sqldrivers.resolvers;

import com.septima.Constants;

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
        put("varchar", Constants.STRING_TYPE_NAME);
        put("int", Constants.NUMBER_TYPE_NAME);
        put("decimal", Constants.NUMBER_TYPE_NAME);
        put("boolean", Constants.BOOLEAN_TYPE_NAME);
        put("timestamp", Constants.DATE_TYPE_NAME);
        put("datetime", Constants.DATE_TYPE_NAME);
        put("geometry", Constants.GEOMETRY_TYPE_NAME);
        put("tinyint", Constants.NUMBER_TYPE_NAME);
        put("smallint", Constants.NUMBER_TYPE_NAME);
        put("mediumint", Constants.NUMBER_TYPE_NAME);
        put("integer", Constants.NUMBER_TYPE_NAME);
        put("bigint", Constants.NUMBER_TYPE_NAME);
        put("serial", Constants.NUMBER_TYPE_NAME);
        put("float", Constants.NUMBER_TYPE_NAME);
        put("real", Constants.NUMBER_TYPE_NAME);
        put("double", Constants.NUMBER_TYPE_NAME);
        put("double precision", Constants.NUMBER_TYPE_NAME);
        put("dec", Constants.NUMBER_TYPE_NAME);
        put("numeric", Constants.NUMBER_TYPE_NAME);
        put("bool", Constants.BOOLEAN_TYPE_NAME);
        put("bit", Constants.BOOLEAN_TYPE_NAME);
        put("char", Constants.STRING_TYPE_NAME);
        put("tinytext", Constants.STRING_TYPE_NAME);
        put("long varchar", Constants.STRING_TYPE_NAME);
        put("text", Constants.STRING_TYPE_NAME);
        put("mediumtext", Constants.STRING_TYPE_NAME);
        put("longtext", Constants.STRING_TYPE_NAME);
        put("date", Constants.DATE_TYPE_NAME);
        put("time", Constants.DATE_TYPE_NAME);
        put("year", Constants.DATE_TYPE_NAME);
        put("enum", Constants.STRING_TYPE_NAME);
        put("set", Constants.STRING_TYPE_NAME);
        put("binary", null);
        put("varbinary", null);
        put("tinyblob", null);
        put("blob", null);
        put("mediumblob", null);
        put("longblob", null);
        put("long varbinary", null);
        // gis types
        put("point", Constants.GEOMETRY_TYPE_NAME);
        put("linestring", Constants.GEOMETRY_TYPE_NAME);
        put("polygon", Constants.GEOMETRY_TYPE_NAME);
        put("multipoint", Constants.GEOMETRY_TYPE_NAME);
        put("multilinestring", Constants.GEOMETRY_TYPE_NAME);
        put("multipolygon", Constants.GEOMETRY_TYPE_NAME);
        put("geometrycollection", Constants.GEOMETRY_TYPE_NAME);
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
