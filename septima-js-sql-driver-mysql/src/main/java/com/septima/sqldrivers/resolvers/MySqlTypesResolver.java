package com.septima.sqldrivers.resolvers;

import com.septima.DataTypes;

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
        put("varchar", DataTypes.STRING_TYPE_NAME);
        put("int", DataTypes.NUMBER_TYPE_NAME);
        put("decimal", DataTypes.NUMBER_TYPE_NAME);
        put("boolean", DataTypes.BOOLEAN_TYPE_NAME);
        put("timestamp", DataTypes.DATE_TYPE_NAME);
        put("datetime", DataTypes.DATE_TYPE_NAME);
        put("geometry", DataTypes.GEOMETRY_TYPE_NAME);
        put("tinyint", DataTypes.NUMBER_TYPE_NAME);
        put("smallint", DataTypes.NUMBER_TYPE_NAME);
        put("mediumint", DataTypes.NUMBER_TYPE_NAME);
        put("integer", DataTypes.NUMBER_TYPE_NAME);
        put("bigint", DataTypes.NUMBER_TYPE_NAME);
        put("serial", DataTypes.NUMBER_TYPE_NAME);
        put("float", DataTypes.NUMBER_TYPE_NAME);
        put("real", DataTypes.NUMBER_TYPE_NAME);
        put("double", DataTypes.NUMBER_TYPE_NAME);
        put("double precision", DataTypes.NUMBER_TYPE_NAME);
        put("dec", DataTypes.NUMBER_TYPE_NAME);
        put("numeric", DataTypes.NUMBER_TYPE_NAME);
        put("bool", DataTypes.BOOLEAN_TYPE_NAME);
        put("bit", DataTypes.BOOLEAN_TYPE_NAME);
        put("char", DataTypes.STRING_TYPE_NAME);
        put("tinytext", DataTypes.STRING_TYPE_NAME);
        put("long varchar", DataTypes.STRING_TYPE_NAME);
        put("text", DataTypes.STRING_TYPE_NAME);
        put("mediumtext", DataTypes.STRING_TYPE_NAME);
        put("longtext", DataTypes.STRING_TYPE_NAME);
        put("date", DataTypes.DATE_TYPE_NAME);
        put("time", DataTypes.DATE_TYPE_NAME);
        put("year", DataTypes.DATE_TYPE_NAME);
        put("enum", DataTypes.STRING_TYPE_NAME);
        put("set", DataTypes.STRING_TYPE_NAME);
        put("binary", null);
        put("varbinary", null);
        put("tinyblob", null);
        put("blob", null);
        put("mediumblob", null);
        put("longblob", null);
        put("long varbinary", null);
        // gis types
        put("point", DataTypes.GEOMETRY_TYPE_NAME);
        put("linestring", DataTypes.GEOMETRY_TYPE_NAME);
        put("polygon", DataTypes.GEOMETRY_TYPE_NAME);
        put("multipoint", DataTypes.GEOMETRY_TYPE_NAME);
        put("multilinestring", DataTypes.GEOMETRY_TYPE_NAME);
        put("multipolygon", DataTypes.GEOMETRY_TYPE_NAME);
        put("geometrycollection", DataTypes.GEOMETRY_TYPE_NAME);
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
