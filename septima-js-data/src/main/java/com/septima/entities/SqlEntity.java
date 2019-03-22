package com.septima.entities;

import com.septima.GenericType;
import com.septima.Database;
import com.septima.dataflow.DataProvider;
import com.septima.metadata.EntityField;
import com.septima.metadata.Parameter;
import com.septima.queries.SqlQuery;

import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Sql based entity with named parameters.
 * <p>
 * This class represents SQL based entity which sql text contains named parameters.
 * Also, it contains parameters' values and type information.
 * It provides a method toQuery() transform transform
 * it transform a SqlQuery replacing parameters names in the query text with "?"
 * placeholders accepted by JDBC, along with parameters values list in
 * the right order.
 *
 * @author mg
 */
public class SqlEntity {

    private static final Pattern NAMED_PARAMETER = Pattern.compile("(?<=[^:]):[a-z]\\w*\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern STRING_LITERAL = Pattern.compile("'[^']*'");
    private static final Pattern MULTILINE_COMMENT = Pattern.compile("/\\*.*\\*/", Pattern.DOTALL);
    private static final Pattern LINE_COMMENT = Pattern.compile("--.*$", Pattern.MULTILINE);

    private final Database database;
    private final String sqlText;
    private final String customSqlText;
    private final String name;
    private final boolean readonly;
    private final boolean command;
    private final boolean procedure;
    private final boolean publicAccess;
    private final String title;
    private final int pageSize;
    private final Map<String, Parameter> params;
    private final Map<String, EntityField> fields;
    private final Set<String> writable;
    private final Set<String> readRoles;
    private final Set<String> writeRoles;

    /**
     * Creates an instance indices Query with given SQL query text. Leaves the
     * parameters map empty.
     *
     * @param aDatabase {@link Database} instance transform use with the new query.
     * @param aSql      the Sql query text.
     */
    public SqlEntity(Database aDatabase, String aSql) {
        this(
                aDatabase,
                aSql,
                null,
                null,
                false,
                false,
                false,
                false,
                null,
                DataProvider.NO_PAGING_PAGE_SIZE,
                Collections.unmodifiableMap(extractParameters(aSql)), Map.of(),
                Set.of(), Set.of(), Set.of()
        );
    }

    public SqlEntity(Database aDatabase,
                     String aSql,
                     String aCustomSql,
                     String aName,
                     boolean aReadonly,
                     boolean aCommand,
                     boolean aProcedure,
                     boolean aPublicAccess,
                     String aTitle,
                     int aPageSize,
                     Map<String, Parameter> aParams,
                     Map<String, EntityField> aFields,
                     Set<String> aWritable,
                     Set<String> aReadRoles,
                     Set<String> aWriteRoles
    ) {
        Objects.requireNonNull(aDatabase, "aDatabase is required argument");
        Objects.requireNonNull(aSql, "aSql is required argument");
        Objects.requireNonNull(aParams, "aParams is required argument");
        Objects.requireNonNull(aFields, "aFields is required argument");
        Objects.requireNonNull(aWritable, "aWritable is required argument");
        Objects.requireNonNull(aReadRoles, "aReadRoles is required argument");
        Objects.requireNonNull(aWriteRoles, "aWriteRoles is required argument");
        database = aDatabase;
        sqlText = aSql;
        customSqlText = aCustomSql;
        name = aName;
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

    public Map<String, EntityField> getFields() {
        return fields;
    }

    public Map<String, Parameter> getParameters() {
        return params;
    }

    public String getTitle() {
        return title;
    }

    public String getName() {
        return name;
    }

    public SqlQuery toQuery() {
        Objects.requireNonNull(sqlText, "Sql query text missing.");
        if (sqlText.isEmpty()) {
            throw new IllegalStateException("Empty Sql query text is not supported");
        }
        if (customSqlText != null && !customSqlText.isEmpty()) {
            Logger.getLogger(SqlEntity.class.getName()).log(Level.INFO, "Entity sql was substituted with customSql while compiling entity {0}", name);
        }
        List<Parameter> compiledParams = new ArrayList<>(params.size());
        String jdbcSql = riddleParameters(
                customSqlText != null && !customSqlText.isEmpty() ? customSqlText : sqlText,
                paramName -> {
                    Parameter p = params.getOrDefault(
                            paramName,
                            new Parameter(paramName)/* null and string type for unbound parameters*/
                    );
                    compiledParams.add(new Parameter(
                            p.getName(),
                            p.getValue(),
                            p.getType(),
                            p.getSubType(),
                            p.getMode(),
                            p.getDescription()
                    ));
                    return database.getSqlDriver().parameterPlaceholder(p);
                }
        );
        return new SqlQuery(
                database,
                name,
                jdbcSql,
                Collections.unmodifiableList(compiledParams),
                procedure,
                pageSize,
                Collections.unmodifiableMap(fields)
        );
    }

    private static String riddleParameters(String aSource, Function<String, String> parameterHandler) {
        return reassemble(
                aSource,
                MULTILINE_COMMENT,
                interMultilineCommentsSegment -> reassemble(
                        interMultilineCommentsSegment,
                        LINE_COMMENT,
                        interLineCommentsSegment -> reassemble(
                                interLineCommentsSegment,
                                STRING_LITERAL,
                                interStringSegment -> reassemble(
                                        interStringSegment,
                                        NAMED_PARAMETER,
                                        Function.identity(),
                                        paramName -> parameterHandler.apply(paramName.substring(1))).toString(),
                                Function.identity()).toString(),
                        Function.identity()).toString(),
                Function.identity()).toString();
    }

    private static StringBuilder reassemble(String aText, Pattern pattern, Function<String, String> replaceInterSegment, Function<String, String> replaceSegment) {
        Matcher matcher = pattern.matcher(aText);
        StringBuilder assembly = new StringBuilder();
        int lastPosition = 0;
        String interSegment;
        while (matcher.find()) {
            interSegment = aText.substring(lastPosition, matcher.start());
            String segment = aText.substring(matcher.start(), matcher.end());
            lastPosition = matcher.end();
            assembly.append(replaceInterSegment.apply((interSegment)));
            assembly.append(replaceSegment.apply(segment));
        }
        interSegment = aText.substring(lastPosition);
        assembly.append(replaceInterSegment.apply((interSegment)));
        return assembly;
    }

    private static Map<String, Parameter> extractParameters(String aSource) {
        Map<String, Parameter> params = new LinkedHashMap<>();
        if (aSource != null && !aSource.isEmpty()) {
            riddleParameters(aSource, paramName -> {
                params.put(paramName, new Parameter(paramName, null, GenericType.STRING));
                return paramName;
            });
        }
        return params;
    }
}
