package com.septima.queries;

import com.septima.ApplicationTypes;
import com.septima.Database;
import com.septima.dataflow.DataProvider;
import com.septima.metadata.Field;
import com.septima.metadata.Parameter;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Sql query with name-bound parameters.
 * <p>
 * <p>
 * This class represents a SQL query which text contains named parameters, and
 * their values with type information. Provides a method compile() to transform
 * it to a SqlCompiledQuery replacing parameters names in the query text to "?"
 * placeholders accepted by JDBC, along with a vector of parameters values in
 * the right order. Method <code>compile()</code> recursively resolves the
 * queries reusing and parameters bindings.</p>
 *
 * @author mg
 */
public class SqlQuery {

    private static final String PARAMETER_NAME_REGEXP = "(?<=[^:]):{1}([A-za-z]\\w*\\b)";
    private final static Pattern PARAMETER_NAME_PATTERN = Pattern.compile(PARAMETER_NAME_REGEXP, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private final static Pattern STRINGS_PATTERN = Pattern.compile("'[^']*'", Pattern.MULTILINE);

    private final Database database;
    private final String sqlText;
    private final String customSqlText;
    private final String entityName;
    private final boolean readonly;
    private final boolean command;
    private final boolean procedure;
    private final boolean publicAccess;
    private final String title;
    private final int pageSize;
    private final Map<String, Parameter> params;
    private final Map<String, Field> fields;
    private final Set<String> writable;
    private final Set<String> readRoles;
    private final Set<String> writeRoles;

    /**
     * Creates an instance of Query with given SQL query text. Leaves the
     * parameters map empty.
     *
     * @param aDatabase {@link Database} instance to use with the new query.
     * @param aSqlText  the Sql query text.
     */
    public SqlQuery(Database aDatabase, String aSqlText) {
        this(
                aDatabase,
                aSqlText,
                null,
                null,
                false,
                false,
                false,
                false,
                null,
                DataProvider.NO_PAGING_PAGE_SIZE,
                Map.of(), Map.of(),
                Set.of(), Set.of(), Set.of()
        );
    }

    public SqlQuery(Database aDatabase,
                    String aSqlText,
                    String aCustomSqlText,
                    String aEntityName,
                    boolean aReadonly,
                    boolean aCommand,
                    boolean aProcedure,
                    boolean aPublicAccess,
                    String aTitle,
                    int aPageSize,
                    Map<String, Parameter> aParams,
                    Map<String, Field> aFields,
                    Set<String> aWritable,
                    Set<String> aReadRoles,
                    Set<String> aWriteRoles
    ) {
        database = aDatabase;
        sqlText = aSqlText;
        customSqlText = aCustomSqlText;
        entityName = aEntityName;
        readonly = aReadonly;
        command = aCommand;
        procedure = aProcedure;
        publicAccess = aPublicAccess;
        title = aTitle;
        pageSize = aPageSize;
        params = aParams;
        fields = aFields;
        writable = aWritable;
        readRoles = aReadRoles;
        writeRoles = aWriteRoles;
    }

    public Database getDatabase() {
        return database;
    }

    public boolean isCommand() {
        return command;
    }

    public boolean isReadonly() {
        return readonly;
    }

    public String getSqlText() {
        return sqlText;
    }

    public String getCustomSqlText() {
        return customSqlText;
    }

    public boolean isPublicAccess() {
        return publicAccess;
    }

    public Set<String> getWritable() {
        return writable;
    }

    public int getPageSize() {
        return pageSize;
    }

    public Set<String> getReadRoles() {
        return readRoles;
    }

    public Set<String> getWriteRoles() {
        return writeRoles;
    }

    public boolean isProcedure() {
        return procedure;
    }

    public Map<String, Field> getFields() {
        return fields;
    }

    public Map<String, Parameter> getParameters() {
        return params;
    }

    public String getTitle() {
        return title;
    }

    public String getEntityName() {
        return entityName;
    }

    /**
     * Compiles the SQL query.
     * <p>
     * <p>
     * The compilation process includes replacing named parameters binding like
     * ":param1" in SQL query text with JDBC "?" placeholders and filling the
     * vector of parameters values according to each parameter occurance in the
     * query.</p>
     * <p>
     * <p>
     * The returned object is able to assign parameters values stored in it to
     * any PreparedStatement object.</p>
     *
     * @return Compiled Sql query.
     * @throws Exception Thrown if any problems with this {@link SqlQuery} occur while compilation, e.g. empty Sql text.
     */
    public SqlCompiledQuery compile() throws Exception {
        if (sqlText == null || sqlText.isEmpty()) {
            throw new Exception("Empty sql query strings are not supported");
        }
        String dialect = database.getSqlDriver().getDialect();
        boolean postgreSQL = dialect.toLowerCase().contains("postgre"); // TODO: Think about how to avoid this hack
        List<Parameter> compiledParams = new ArrayList<>(params.size());
        StringBuilder compiledSb = new StringBuilder();
        Matcher sm = STRINGS_PATTERN.matcher(sqlText);
        String[] withoutStrings = sqlText.split("('[^']*')");
        for (int i = 0; i < withoutStrings.length; i++) {
            Matcher m = PARAMETER_NAME_PATTERN.matcher(withoutStrings[i]);
            StringBuffer withoutStringsSegment = new StringBuffer();
            while (m.find()) {
                String paramName = m.group(1);
                Parameter p = params.getOrDefault(
                        paramName,
                        new Parameter(paramName)/* null for unbound parameters*/
                );
                compiledParams.add(new Parameter(
                        p.getName(),
                        p.getDescription(),
                        p.getType(),
                        p.getValue(),
                        p.isModified(),
                        p.getMode()
                ));
                m.appendReplacement(withoutStringsSegment, postgreSQL && ApplicationTypes.DATE_TYPE_NAME.equals(p.getType()) ? "?::timestamp" : "?");
            }
            m.appendTail(withoutStringsSegment);
            withoutStrings[i] = withoutStringsSegment.toString();
            compiledSb.append(withoutStrings[i]);
            if (sm.find()) {
                compiledSb.append(sm.group(0));
            }
        }
        return new SqlCompiledQuery(database, entityName, compiledSb.toString(), compiledParams, fields, pageSize, procedure);
    }

    protected static Map<String, Parameter> extractParameters(String sqlText) {
        Map<String, Parameter> params = new LinkedHashMap<>();
        if (sqlText != null && !sqlText.isEmpty()) {
            Pattern pattern = Pattern.compile(PARAMETER_NAME_REGEXP, Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(sqlText);
            while (matcher.find()) {
                String paramName = sqlText.substring(matcher.start() + 1, matcher.end());
                Parameter parameter = new Parameter(paramName, "", ApplicationTypes.STRING_TYPE_NAME);
                params.put(parameter.getName(), parameter);
            }
        }
        return params;
    }
}
