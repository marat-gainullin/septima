package com.septima.queries;

import com.septima.Constants;
import com.septima.Database;
import com.septima.DataSources;
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

    private final static Pattern PARAMETER_NAME_PATTERN = Pattern.compile(Constants.PARAMETER_NAME_REGEXP, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private final static Pattern STRINGS_PATTERN = Pattern.compile("'[^']*'", Pattern.MULTILINE);

    // parameters propagation. ParamName, QueryName, ParamName
    protected Map<String, Map<String, String>> parametersBinds = new HashMap<>();
    protected Map<String, Field> fields = new LinkedHashMap<>();
    protected Map<String, Parameter> params = new LinkedHashMap<>();
    protected String title;
    protected String entityName;
    protected boolean procedure;
    protected Set<String> readRoles = new HashSet<>();
    protected Set<String> writeRoles = new HashSet<>();
    // Joins, conditions, parametersList, groups, havings etc.
    protected String sqlText;
    // the same as sqlText, but it is used when very custom sql is needed.
    protected String fullSqlText;
    protected Set<String> writable;
    protected int pageSize = DataProvider.NO_PAGING_PAGE_SIZE;
    protected boolean publicAccess;
    protected boolean command;
    protected Database database;

    /**
     * Creates an instance of Query with empty SQL query text and parameters
     * map.
     *
     * @param aDatabase
     */
    public SqlQuery(Database aDatabase) {
        super();
        database = aDatabase;
    }

    /**
     * Creates an instance of Query with given SQL query text. Leaves the
     * parameters map empty.
     *
     * @param aDatabase
     * @param aSqlText  the SQL query text.
     */
    public SqlQuery(Database aDatabase, String aSqlText) {
        this(aDatabase);
        sqlText = aSqlText;
    }

    public Database getDatabase() {
        return database;
    }

    public boolean isCommand() {
        return command;
    }

    public String getSqlText() {
        return sqlText;
    }

    public String getFullSqlText() {
        return fullSqlText;
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

    public Map<String, Map<String, String>> getParametersBinds() {
        return parametersBinds;
    }

    public String getEntityName() {
        return entityName;
    }

    public void putParameter(String aName, String aType, Object aValue) {
        Parameter param = new Parameter();
        param.setName(aName);
        param.setType(aType);
        param.setDefaultValue(aValue);
        param.setValue(aValue);
        params.put(param.getName(), param);
    }

    public void putParameter(String aName, String aType, Object aDefaultValue, Object aValue) {
        Parameter param = new Parameter();
        param.setName(aName);
        param.setType(aType);
        param.setDefaultValue(aDefaultValue);
        param.setValue(aValue);
        params.put(param.getName(), param);
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
     * @throws Exception
     */
    public SqlCompiledQuery compile() throws Exception {
        if (sqlText == null || sqlText.isEmpty()) {
            throw new Exception("Empty sql query strings are not supported");
        }
        String dialect = database.getSqlDriver().getDialect();
        boolean postgreSQL = Constants.POSTGRE_DIALECT.equals(dialect);
        List<Parameter> compiledParams = new ArrayList();
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
                        p.getDefaultValue(),
                        p.isModified(),
                        p.getMode()
                ));
                m.appendReplacement(withoutStringsSegment, postgreSQL && Constants.DATE_TYPE_NAME.equals(p.getType()) ? "?::timestamp" : "?");
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
        Map<String, Parameter> params = new LinkedHashMap();
        if (sqlText != null && !sqlText.isEmpty()) {
            Pattern pattern = Pattern.compile(Constants.PARAMETER_NAME_REGEXP, Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(sqlText);
            while (matcher.find()) {
                String paramName = sqlText.substring(matcher.start() + 1, matcher.end());
                Parameter parameter = new Parameter(paramName, "", Constants.STRING_TYPE_NAME);
                params.put(parameter.getName(), parameter);
            }
        }
        return params;
    }
}
