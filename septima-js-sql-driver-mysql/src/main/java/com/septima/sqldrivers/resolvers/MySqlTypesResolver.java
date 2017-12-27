package com.septima.sqldrivers.resolvers;

import com.septima.GenericType;

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

    private static final Map<String, GenericType> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>(){{
        put("varchar", GenericType.STRING);
        put("char", GenericType.STRING);
        put("tinytext", GenericType.STRING);
        put("enum", GenericType.STRING);
        put("set", GenericType.STRING);
        put("long varchar", GenericType.STRING);
        put("text", GenericType.STRING);
        put("mediumtext", GenericType.STRING);
        put("longtext", GenericType.STRING);
        put("int", GenericType.LONG);
        put("tinyint", GenericType.LONG);
        put("smallint", GenericType.LONG);
        put("mediumint", GenericType.LONG);
        put("integer", GenericType.LONG);
        put("bigint", GenericType.LONG);
        put("serial", GenericType.LONG);
        put("decimal", GenericType.DOUBLE);
        put("float", GenericType.DOUBLE);
        put("real", GenericType.DOUBLE);
        put("double", GenericType.DOUBLE);
        put("double precision", GenericType.DOUBLE);
        put("dec", GenericType.DOUBLE);
        put("numeric", GenericType.DOUBLE);
        put("bool", GenericType.BOOLEAN);
        put("bit", GenericType.BOOLEAN);
        put("boolean", GenericType.BOOLEAN);
        put("timestamp", GenericType.DATE);
        put("datetime", GenericType.DATE);
        put("date", GenericType.DATE);
        put("time", GenericType.DATE);
        put("year", GenericType.DATE);
        put("geometry", GenericType.GEOMETRY);
        put("binary", null);
        put("varbinary", null);
        put("tinyblob", null);
        put("blob", null);
        put("mediumblob", null);
        put("longblob", null);
        put("long varbinary", null);
        // gis types
        put("point", GenericType.GEOMETRY);
        put("linestring", GenericType.GEOMETRY);
        put("polygon", GenericType.GEOMETRY);
        put("multipoint", GenericType.GEOMETRY);
        put("multilinestring", GenericType.GEOMETRY);
        put("multipolygon", GenericType.GEOMETRY);
        put("geometrycollection", GenericType.GEOMETRY);
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
