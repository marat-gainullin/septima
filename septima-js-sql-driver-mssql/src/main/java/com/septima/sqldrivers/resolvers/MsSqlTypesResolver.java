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
public class MsSqlTypesResolver implements TypesResolver {

    private static final Map<String, String> rdbmsTypes2ApplicationTypes = new LinkedHashMap<>() {{
        put("varchar", ApplicationTypes.STRING_TYPE_NAME);
        put("numeric", ApplicationTypes.NUMBER_TYPE_NAME);
        put("decimal", ApplicationTypes.NUMBER_TYPE_NAME);
        put("money", ApplicationTypes.NUMBER_TYPE_NAME);
        put("bit", ApplicationTypes.BOOLEAN_TYPE_NAME);
        put("datetime", ApplicationTypes.DATE_TYPE_NAME);
        put("int", ApplicationTypes.NUMBER_TYPE_NAME);
        put("smallint", ApplicationTypes.NUMBER_TYPE_NAME);
        put("tinyint", ApplicationTypes.NUMBER_TYPE_NAME);
        put("bigint", ApplicationTypes.NUMBER_TYPE_NAME);
        put("real", ApplicationTypes.NUMBER_TYPE_NAME);
        put("float", ApplicationTypes.NUMBER_TYPE_NAME);
        put("smallmoney", ApplicationTypes.NUMBER_TYPE_NAME);
        put("tinyint identity", ApplicationTypes.NUMBER_TYPE_NAME);
        put("bigint identity", ApplicationTypes.NUMBER_TYPE_NAME);
        put("numeric identity", ApplicationTypes.NUMBER_TYPE_NAME);
        put("decimal identity", ApplicationTypes.NUMBER_TYPE_NAME);
        put("int identity", ApplicationTypes.NUMBER_TYPE_NAME);
        put("smallint identity", ApplicationTypes.NUMBER_TYPE_NAME);
        put("nvarchar", ApplicationTypes.STRING_TYPE_NAME);
        put("char", ApplicationTypes.STRING_TYPE_NAME);
        put("nchar", ApplicationTypes.STRING_TYPE_NAME);

        put("smalldatetime", ApplicationTypes.DATE_TYPE_NAME);
        put("datetime2", ApplicationTypes.DATE_TYPE_NAME);
        put("date", ApplicationTypes.DATE_TYPE_NAME);
        put("time", ApplicationTypes.DATE_TYPE_NAME);
        put("text", ApplicationTypes.STRING_TYPE_NAME);
        put("ntext", ApplicationTypes.STRING_TYPE_NAME);
        put("uniqueidentifier", ApplicationTypes.STRING_TYPE_NAME);
        put("sysname", ApplicationTypes.STRING_TYPE_NAME);
        put("xml", ApplicationTypes.STRING_TYPE_NAME);
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