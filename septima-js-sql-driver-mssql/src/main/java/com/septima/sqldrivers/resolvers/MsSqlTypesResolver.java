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
public class MsSqlTypesResolver implements TypesResolver {

    private static final Map<String, String> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>() {{
        put("varchar", DataTypes.STRING_TYPE_NAME);
        put("numeric", DataTypes.NUMBER_TYPE_NAME);
        put("decimal", DataTypes.NUMBER_TYPE_NAME);
        put("money", DataTypes.NUMBER_TYPE_NAME);
        put("bit", DataTypes.BOOLEAN_TYPE_NAME);
        put("datetime", DataTypes.DATE_TYPE_NAME);
        put("int", DataTypes.NUMBER_TYPE_NAME);
        put("smallint", DataTypes.NUMBER_TYPE_NAME);
        put("tinyint", DataTypes.NUMBER_TYPE_NAME);
        put("bigint", DataTypes.NUMBER_TYPE_NAME);
        put("real", DataTypes.NUMBER_TYPE_NAME);
        put("float", DataTypes.NUMBER_TYPE_NAME);
        put("smallmoney", DataTypes.NUMBER_TYPE_NAME);
        put("tinyint identity", DataTypes.NUMBER_TYPE_NAME);
        put("bigint identity", DataTypes.NUMBER_TYPE_NAME);
        put("numeric identity", DataTypes.NUMBER_TYPE_NAME);
        put("decimal identity", DataTypes.NUMBER_TYPE_NAME);
        put("int identity", DataTypes.NUMBER_TYPE_NAME);
        put("smallint identity", DataTypes.NUMBER_TYPE_NAME);
        put("nvarchar", DataTypes.STRING_TYPE_NAME);
        put("char", DataTypes.STRING_TYPE_NAME);
        put("nchar", DataTypes.STRING_TYPE_NAME);

        put("smalldatetime", DataTypes.DATE_TYPE_NAME);
        put("datetime2", DataTypes.DATE_TYPE_NAME);
        put("date", DataTypes.DATE_TYPE_NAME);
        put("time", DataTypes.DATE_TYPE_NAME);
        put("text", DataTypes.STRING_TYPE_NAME);
        put("ntext", DataTypes.STRING_TYPE_NAME);
        put("uniqueidentifier", DataTypes.STRING_TYPE_NAME);
        put("sysname", DataTypes.STRING_TYPE_NAME);
        put("xml", DataTypes.STRING_TYPE_NAME);
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
    public String toApplicationType(int aJdbcType, String aRdbmsTypeName) {
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