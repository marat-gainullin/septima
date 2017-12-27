package com.septima.sqldrivers.resolvers;

import com.septima.GenericType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author mg
 */
public class MsSqlTypesResolver implements TypesResolver {

    private static final Map<String, GenericType> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>() {{
        put("varchar", GenericType.STRING);
        put("nvarchar", GenericType.STRING);
        put("char", GenericType.STRING);
        put("nchar", GenericType.STRING);
        put("text", GenericType.STRING);
        put("ntext", GenericType.STRING);
        put("uniqueidentifier", GenericType.STRING);
        put("sysname", GenericType.STRING);
        put("xml", GenericType.STRING);
        put("int", GenericType.LONG);
        put("smallint", GenericType.LONG);
        put("tinyint", GenericType.LONG);
        put("bigint", GenericType.LONG);
        put("tinyint identity", GenericType.LONG);
        put("bigint identity", GenericType.LONG);
        put("int identity", GenericType.LONG);
        put("smallint identity", GenericType.LONG);
        put("numeric", GenericType.DOUBLE);
        put("decimal", GenericType.DOUBLE);
        put("money", GenericType.DOUBLE);
        put("real", GenericType.DOUBLE);
        put("float", GenericType.DOUBLE);
        put("smallmoney", GenericType.DOUBLE);
        put("numeric identity", GenericType.DOUBLE);
        put("decimal identity", GenericType.DOUBLE);

        put("bit", GenericType.BOOLEAN);
        put("datetime", GenericType.DATE);
        put("smalldatetime", GenericType.DATE);
        put("datetime2", GenericType.DATE);
        put("date", GenericType.DATE);
        put("time", GenericType.DATE);
        put("image", null);
        put("sql_variant", null);
        put("varbinary", null);
        put("binary", null);
    }};
    private static final Set<String> jdbcTypesWithSize = new HashSet<>() {{
        add("char");
        add("varchar");
        add("nchar");
        add("nvarchar");
        add("binary");
        add("varbinary");
    }};
    private static final Set<String> jdbcTypesWithScale = new HashSet<>();
    private static final Map<String, Integer> jdbcTypesMaxSize = new HashMap<>() {{
        put("char", 8000);
        put("nchar", 4000);
        put("varchar", 8000);
        put("nvarchar", 4000);
        put("binary", 8000);
        put("varbinary", 8000);
    }};
    private static final Map<String, Integer> jdbcTypesDefaultSize = new HashMap<>() {{
        put("char", 1);
        put("nchar", 1);
        put("varchar", 200);
        put("nvarchar", 200);
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
    public boolean isSized(String aRDBMSType) {
        return jdbcTypesWithSize.contains(aRDBMSType.toLowerCase());
    }

    @Override
    public boolean isScaled(String aRDBMSType) {
        return jdbcTypesWithScale.contains(aRDBMSType.toLowerCase());
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