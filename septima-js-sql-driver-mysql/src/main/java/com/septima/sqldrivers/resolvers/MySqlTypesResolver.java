package com.septima.sqldrivers.resolvers;

import com.septima.application.ApplicationDataTypes;

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
        put("varchar", ApplicationDataTypes.STRING_TYPE_NAME);
        put("int", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("decimal", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("boolean", ApplicationDataTypes.BOOLEAN_TYPE_NAME);
        put("timestamp", ApplicationDataTypes.DATE_TYPE_NAME);
        put("datetime", ApplicationDataTypes.DATE_TYPE_NAME);
        put("geometry", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("tinyint", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("smallint", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("mediumint", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("integer", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("bigint", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("serial", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("float", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("real", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("double", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("double precision", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("dec", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("numeric", ApplicationDataTypes.NUMBER_TYPE_NAME);
        put("bool", ApplicationDataTypes.BOOLEAN_TYPE_NAME);
        put("bit", ApplicationDataTypes.BOOLEAN_TYPE_NAME);
        put("char", ApplicationDataTypes.STRING_TYPE_NAME);
        put("tinytext", ApplicationDataTypes.STRING_TYPE_NAME);
        put("long varchar", ApplicationDataTypes.STRING_TYPE_NAME);
        put("text", ApplicationDataTypes.STRING_TYPE_NAME);
        put("mediumtext", ApplicationDataTypes.STRING_TYPE_NAME);
        put("longtext", ApplicationDataTypes.STRING_TYPE_NAME);
        put("date", ApplicationDataTypes.DATE_TYPE_NAME);
        put("time", ApplicationDataTypes.DATE_TYPE_NAME);
        put("year", ApplicationDataTypes.DATE_TYPE_NAME);
        put("enum", ApplicationDataTypes.STRING_TYPE_NAME);
        put("set", ApplicationDataTypes.STRING_TYPE_NAME);
        put("binary", null);
        put("varbinary", null);
        put("tinyblob", null);
        put("blob", null);
        put("mediumblob", null);
        put("longblob", null);
        put("long varbinary", null);
        // gis types
        put("point", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("linestring", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("polygon", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("multipoint", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("multilinestring", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("multipolygon", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
        put("geometrycollection", ApplicationDataTypes.GEOMETRY_TYPE_NAME);
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
