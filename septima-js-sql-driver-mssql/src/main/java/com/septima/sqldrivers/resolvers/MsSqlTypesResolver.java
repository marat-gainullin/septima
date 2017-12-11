package com.septima.sqldrivers.resolvers;

import com.septima.Constants;

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
        put("varchar", Constants.STRING_TYPE_NAME);
        put("numeric", Constants.NUMBER_TYPE_NAME);
        put("decimal", Constants.NUMBER_TYPE_NAME);
        put("money", Constants.NUMBER_TYPE_NAME);
        put("bit", Constants.BOOLEAN_TYPE_NAME);
        put("datetime", Constants.DATE_TYPE_NAME);
        put("int", Constants.NUMBER_TYPE_NAME);
        put("smallint", Constants.NUMBER_TYPE_NAME);
        put("tinyint", Constants.NUMBER_TYPE_NAME);
        put("bigint", Constants.NUMBER_TYPE_NAME);
        put("real", Constants.NUMBER_TYPE_NAME);
        put("float", Constants.NUMBER_TYPE_NAME);
        put("smallmoney", Constants.NUMBER_TYPE_NAME);
        put("tinyint identity", Constants.NUMBER_TYPE_NAME);
        put("bigint identity", Constants.NUMBER_TYPE_NAME);
        put("numeric identity", Constants.NUMBER_TYPE_NAME);
        put("decimal identity", Constants.NUMBER_TYPE_NAME);
        put("int identity", Constants.NUMBER_TYPE_NAME);
        put("smallint identity", Constants.NUMBER_TYPE_NAME);
        put("nvarchar", Constants.STRING_TYPE_NAME);
        put("char", Constants.STRING_TYPE_NAME);
        put("nchar", Constants.STRING_TYPE_NAME);

        put("smalldatetime", Constants.DATE_TYPE_NAME);
        put("datetime2", Constants.DATE_TYPE_NAME);
        put("date", Constants.DATE_TYPE_NAME);
        put("time", Constants.DATE_TYPE_NAME);
        put("text", Constants.STRING_TYPE_NAME);
        put("ntext", Constants.STRING_TYPE_NAME);
        put("uniqueidentifier", Constants.STRING_TYPE_NAME);
        put("sysname", Constants.STRING_TYPE_NAME);
        put("xml", Constants.STRING_TYPE_NAME);
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
    public String toApplicationType(int aJdbcType, String aRDBMSType) {
        return aRDBMSType != null ? rdbmsTypes2ApplicationTypes.get(aRDBMSType.toLowerCase()) : null;
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