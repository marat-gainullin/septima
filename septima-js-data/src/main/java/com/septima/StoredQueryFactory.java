package com.septima;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.septima.dataflow.DataProvider;
import com.septima.metadata.Field;
import com.septima.metadata.ForeignKey;
import com.septima.metadata.JdbcColumn;
import com.septima.metadata.Parameter;
import com.septima.queries.SqlQuery;
import com.septima.sqldrivers.resolvers.TypesResolver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.ResultsFinder;
import net.sf.jsqlparser.SourcesFinder;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

/**
 * @author mg.
 */
public class StoredQueryFactory {

    public static final String _Q = "\\" + Constants.STORED_QUERY_REF_PREFIX + "?";

    private Fields processSubQuery(SqlQuery aQuery, SubSelect aSubSelect) throws Exception {
        SqlQuery subQuery = new SqlQuery(aQuery.getDatabase(), "", );
        subQuery.setEntityName(aSubSelect.getAliasName());
        resolveOutputFieldsFromTables(subQuery, aSubSelect.getSelectBody());
        return subQuery.getFields();
    }

    public static final String INNER_JOIN_CONSTRUCTION = "select %s from %s %s inner join %s on (%s.%s = %s.%s)";
    public static final String ABSENT_QUERY_MSG = "Query %s is not found";
    public static final String CANT_LOAD_NULL_MSG = "Query name is null.";
    public static final String COLON = ":";
    public static final String CONTENT_EMPTY_MSG = "Content of %s is empty";
    public static final String DUMMY_FIELD_NAME = "dummy";
    public static final String INEER_JOIN_CONSTRUCTING_MSG = "Constructing query with left Query %s and right table %s";
    public static final String LOADING_QUERY_MSG = "Loading stored query %s";
    private final Database database;
    private final Path applicationPath;
    private boolean aliasesToTableNames;

    public boolean isAliasesToTableNames() {
        return aliasesToTableNames;
    }

    public void setAliasesToTableNames(boolean aValue) {
        aliasesToTableNames = aValue;
    }

    protected void addTableFieldsToSelectResults(SqlQuery aQuery, Table aFieldsSource) throws Exception {
        FieldsResult fieldsRes = getTablyFields(aQuery.getDatasourceName(), aFieldsSource.getWholeTableName());
        if (fieldsRes != null && fieldsRes.fields != null) {
            Metadata mdCache = database.getMetadataCache(aQuery.getDatasourceName());
            if (mdCache != null) {
                TypesResolver resolver = mdCache.getDataSourceSqlDriver().getTypesResolver();
                fieldsRes.fields.toCollection().stream().forEach((Field field) -> {
                    Field copied = new Field();
                    copied.assignFrom(field);
                    if (fieldsRes.fromRealTable) {
                        JdbcColumn jField = (JdbcColumn) field;
                        copied.setType(resolver.toApplicationType(jField.getJdbcType(), jField.getType()));
                        if (jField.getSchemaName() != null && !jField.getSchemaName().isEmpty()) {
                            copied.setTableName(jField.getSchemaName() + "." + copied.getTableName());
                        }
                    }
                    /**
                     * Заменять имя оригинальной таблицы нельзя, особенно если
                     * это поле ключевое т.к. при установлении связи по этим
                     * полям будут проблемы. ORM-у придется "разматывать"
                     * источник поля до таблицы чтобы восстановиит связи по
                     * ключам. Здесь это делается исключительно ради очень
                     * специального использования фабрики в дизайнере запросов.
                     */
                    if (aliasesToTableNames
                            && aFieldsSource.getAlias() != null && !aFieldsSource.getAlias().getName().isEmpty()) {
                        copied.setTableName(aFieldsSource.getAlias().getName());
                    }
                    aQuery.getFields().add(copied);
                });
            }
        }
    }

    public static Map<String, FromItem> prepareUniqueTables(Map<String, FromItem> tables) {
        Map<String, FromItem> uniqueTables = new HashMap<>();
        tables.values().stream().forEach((fromItem) -> {
            if (fromItem.getAlias() != null && !fromItem.getAlias().getName().isEmpty()) {
                uniqueTables.put(fromItem.getAlias().getName().toLowerCase(), fromItem);
            } else if (fromItem instanceof Table) {
                uniqueTables.put(((Table) fromItem).getWholeTableName().toLowerCase(), fromItem);
            }
        });
        return uniqueTables;
    }

    public SqlQuery loadQuery(String aEntityName) throws Exception {
        if (aEntityName != null) {
            Logger.getLogger(this.getClass().getName()).finer(String.format(LOADING_QUERY_MSG, aEntityName));
            String filyName = aEntityName.replace(".", File.separator) + ".sql";
            File mainQueryFile = applicationPath.resolve(filyName).toFile();
            if (mainQueryFile.exists()) {
                if (!mainQueryFile.isDirectory()) {
                    return fileToSqlQuery(aEntityName, mainQueryFile);
                } else {
                    throw new IllegalStateException(filyName + "' at path: " + applicationPath + " is a directory.");
                }
            } else {
                throw new FileNotFoundException("Can't find '" + filyName + "' from path: " + applicationPath);
            }
        } else {
            throw new NullPointerException(CANT_LOAD_NULL_MSG);
        }
    }

    protected SqlQuery fileToSqlQuery(String anEntityName, File aFile) throws Exception {
        String sql = new String(Files.readAllBytes(aFile.toPath()), StandardCharsets.UTF_8);
        if (!sql.isEmpty()) {
            String title;
            String customSql;
            boolean command;
            boolean procedure;
            boolean readonly;
            boolean publicAccess;
            int pageSize;

            Map<String, Field> fields = new LinkedHashMap<>();
            fillFieldsFromMetadata(fields);
            Map<String, Parameter> params = new LinkedHashMap<>();
            Map<String, Map<String, Set<String>>> paramsBinds = new HashMap<>();
            Set<String> writable = new HashSet<>();
            Set<String> readRoles = new HashSet<>();
            Set<String> writeRoles = new HashSet<>();

            File jsonFile = aFile.getParentFile().toPath().resolve(aFile.getName() + ".json").toFile();
            if (jsonFile.exists() && !jsonFile.isDirectory()) {
                String json = new String(Files.readAllBytes(jsonFile.toPath()), StandardCharsets.UTF_8);
                ObjectMapper jsonMapper = new ObjectMapper();
                JsonNode entityDocument = jsonMapper.readTree(json);
                JsonNode titleNode = entityDocument.get("title");
                title = titleNode != null && titleNode.isTextual() ? titleNode.asText() : anEntityName;
                JsonNode sqlNode = entityDocument.get("sql");
                customSql = sqlNode != null && sqlNode.isTextual() ? sqlNode.asText() : null;
                JsonNode commandNode = entityDocument.get("command");
                command = commandNode != null && commandNode.asBoolean();
                JsonNode procedureNode = entityDocument.get("procedure");
                procedure = procedureNode != null && procedureNode.asBoolean();
                JsonNode readonlyNode = entityDocument.get("readonly");
                readonly = readonlyNode != null && readonlyNode.asBoolean();
                JsonNode publicNode = entityDocument.get("public");
                publicAccess = publicNode != null && publicNode.asBoolean();
                JsonNode pageSizeNode = entityDocument.get("pageSize");
                pageSize = pageSizeNode != null && pageSizeNode.isInt() ? pageSizeNode.asInt(DataProvider.NO_PAGING_PAGE_SIZE) : DataProvider.NO_PAGING_PAGE_SIZE;
                JsonNode paramsNode = entityDocument.get("parameters");
                if (paramsNode != null && paramsNode.isObject()) {
                    StreamSupport.stream(Spliterators.spliterator(
                            paramsNode.fields(), paramsNode.size(), 0), false)
                            .forEachOrdered(parameterEntry -> {
                                String parameterName = parameterEntry.getKey();
                                Parameter parameter = params.getOrDefault(parameterName, new Parameter(parameterName));
                                JsonNode parameterNode = parameterEntry.getValue();
                                JsonNode typeNode = parameterNode.get("type");
                                String type = typeNode != null && typeNode.isTextual() ? typeNode.asText(parameter.getType()) : parameter.getType();
                                JsonNode descriptionNode = parameterNode.get("description");
                                String description = descriptionNode != null && descriptionNode.isTextual() ? descriptionNode.asText(parameter.getDescription()) : parameter.getDescription();
                                JsonNode defaultValueNode = parameterNode.get("value");
                                String defaultValue = defaultValueNode != null && defaultValueNode.isTextual() ? defaultValueNode.asText() : null;
                                params.put(parameterName, new Parameter(
                                        parameterName,
                                        description,
                                        type
                                ));
                                // Binds
                                JsonNode bindsNode = parameterNode.get("binds");
                                if (bindsNode != null && bindsNode.isObject()) {
                                    StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                                            bindsNode.fields(), 0), false)
                                            .forEachOrdered(subQueryEntry -> {
                                                String subQueryName = subQueryEntry.getKey();
                                                JsonNode subQueryParamsNode = subQueryEntry.getValue();
                                                if (subQueryParamsNode.isArray()) {
                                                    for (int i = 0; i < subQueryParamsNode.size(); i++) {
                                                        JsonNode subQueryParamNode = subQueryParamsNode.get(i);
                                                        if (subQueryParamNode.isTextual()) {
                                                            Map<String, Set<String>> paramBinds = paramsBinds.computeIfAbsent(parameterName, name -> new HashMap<>());
                                                            Set<String> subQueryParams = paramBinds.computeIfAbsent(subQueryName, name -> new HashSet<>());
                                                            subQueryParams.add(subQueryParamNode.asText());
                                                        }
                                                    }
                                                }
                                            });
                                }
                            });
                }
                JsonNode fieldsNode = entityDocument.get("fields");
                StreamSupport.stream(Spliterators.spliterator(
                        fieldsNode.fields(), fieldsNode.size(), 0), false)
                        .forEachOrdered(fieldEntry -> {
                            String fieldName = fieldEntry.getKey();
                            Field field = fields.getOrDefault(fieldName, new Field(fieldName));
                            JsonNode fieldNode = fieldEntry.getValue();
                            JsonNode typeNode = fieldNode.get("type");
                            String type = typeNode != null && typeNode.isTextual() ? typeNode.asText(field.getType()) : field.getType();
                            JsonNode descriptionNode = fieldNode.get("description");
                            String description = descriptionNode != null && descriptionNode.isTextual() ? descriptionNode.asText(field.getDescription()) : field.getDescription();
                            JsonNode nullableNode = entityDocument.get("nullable");
                            boolean nullable = nullableNode != null && nullableNode.isBoolean() ? nullableNode.asBoolean() : field.isNullable();
                            JsonNode originalNameNode = fieldNode.get("originalName");
                            String originalName = originalNameNode != null && originalNameNode.isTextual() ? originalNameNode.asText(field.getOriginalName()) : field.getOriginalName();
                            JsonNode tableNameNode = fieldNode.get("tableName");
                            String tableName = tableNameNode != null && tableNameNode.isTextual() ? tableNameNode.asText(field.getTableName()) : field.getTableName();
                            JsonNode keyNode = fieldNode.get("key");
                            boolean key = keyNode != null && keyNode.isBoolean() ? keyNode.asBoolean() : field.isPk();
                            String referencedEntity;
                            String referencedKey;
                            JsonNode referenceNode = fieldNode.get("reference");
                            if (referenceNode != null && referenceNode.isObject()) {
                                JsonNode referencedEntityNode = referenceNode.get("entity");
                                JsonNode referencedKeyNode = referenceNode.get("key");
                                if (referencedEntityNode != null &&
                                        referencedKeyNode != null &&
                                        referencedEntityNode.isTextual() &&
                                        referencedKeyNode.isTextual()
                                        ) {
                                    referencedEntity = referencedEntityNode.asText();
                                    referencedKey = referencedKeyNode.asText();
                                } else {
                                    referencedEntity = null;
                                    referencedKey = null;
                                }
                            } else {
                                referencedEntity = null;
                                referencedKey = null;
                            }
                            fields.put(fieldName, new Field(
                                            fieldName,
                                            description,
                                            originalName,
                                            tableName,
                                            type,
                                            nullable,
                                            key,
                                            referencedEntity != null && !referencedEntity.isEmpty() &&
                                                    referencedKey != null && !referencedKey.isEmpty() ?
                                                    new ForeignKey(
                                                            null, anEntityName, fieldName, null,
                                                            ForeignKey.ForeignKeyRule.NOACTION, ForeignKey.ForeignKeyRule.NOACTION, false,
                                                            null, referencedEntity, referencedKey, null)
                                                    : field.getFk()
                                    )
                            );
                        });

                JsonNode writableNode = entityDocument.get("writable");
                if (writableNode != null && writableNode.isArray()) {
                    for (int i = 0; i < writableNode.size(); i++) {
                        JsonNode writableTableNode = writableNode.get(i);
                        if (writableTableNode.isTextual()) {
                            String writableTable = writableTableNode.asText();
                            if (writableTable != null && !writableTable.isEmpty()) {
                                writable.add(writableTable);
                            }
                        }
                    }
                }
                JsonNode rolesNode = entityDocument.get("roles");
                if (rolesNode != null && rolesNode.isObject()) {
                    JsonNode readRolesNode = rolesNode.get("read");
                    if (readRolesNode != null && readRolesNode.isArray()) {
                        for (int i = 0; i < readRolesNode.size(); i++) {
                            JsonNode readRoleNode = readRolesNode.get(i);
                            if (readRoleNode.isTextual()) {
                                String readRole = readRoleNode.asText();
                                if (readRole != null && !readRole.isEmpty()) {
                                    readRoles.add(readRole);
                                }
                            }
                        }
                    }
                    JsonNode writeRolesNode = rolesNode.get("write");
                    if (writeRolesNode != null && writeRolesNode.isArray()) {
                        for (int i = 0; i < writeRolesNode.size(); i++) {
                            JsonNode writeRoleNode = writeRolesNode.get(i);
                            if (writeRoleNode.isTextual()) {
                                String writeRole = writeRoleNode.asText();
                                if (writeRole != null && !writeRole.isEmpty()) {
                                    writeRoles.add(writeRole);
                                }
                            }
                        }
                    }
                }
            } else {
                title = null;
                customSql = null;
                readonly = false;
                command = false;
                procedure = false;
                publicAccess = false;
                pageSize = DataProvider.NO_PAGING_PAGE_SIZE;
            }
            String sqlWithSubQueries = inlineSubQueries(sql, paramsBinds);
            return new SqlQuery(
                    database,
                    sqlWithSubQueries,
                    customSql,
                    anEntityName,
                    readonly,
                    command,
                    procedure,
                    publicAccess,
                    title,
                    pageSize,
                    Collections.unmodifiableMap(params),
                    Collections.unmodifiableMap(fields),
                    Collections.unmodifiableSet(writable),
                    Collections.unmodifiableSet(readRoles),
                    Collections.unmodifiableSet(writeRoles)
            );
        } else {
            throw new IllegalStateException(anEntityName + " has an empty sql text.");
        }
    }

    /**
     * Constructs factory for queries stored in application database;
     *
     * @throws java.lang.Exception
     */
    public StoredQueryFactory(Database aDatabase, Path aApplicationPath) throws Exception {
        super();
        database = aDatabase;
        applicationPath = aApplicationPath;
    }

    /**
     * Заменяет в запросе ссылки на подзапросы на их содержимое. Подставляет
     * параметры запроса в соответствии со связями в параметры подзапросов.
     *
     * @param aSqlText
     * @param aBinds
     * @return Запрос без ссылок на подзапросы.
     * @throws java.lang.Exception
     */
    public String inlineSubQueries(String aSqlText, Map<String, Map<String, String>> aBinds) throws Exception {
        /**
         * Старая реализация заменяла текст всех подзапросов с одним и тем же
         * идентификатором, не обращая внимания на алиасы. Поэтому запросы
         * содержащие в себе один и тот же подзапрос несколько раз,
         * компилировались неправильно. Неправильно подставлялись и параметры.
         */
        assert aBinds != null;
        if (aBinds.getEntities() != null) {
            String processedSql = aSqlText;
            for (QueryEntity entity : aBinds.getEntities().values()) {
                assert entity != null;
                if (entity.getQueryName() != null) {
                    String queryTablyName = entity.getQueryName();
                    Pattern subQueryPattern = Pattern.compile(_Q + queryTablyName, Pattern.CASE_INSENSITIVE);
                    String tAlias = entity.getAlias();
                    if (tAlias != null && !tAlias.isEmpty()) {
                        subQueryPattern = Pattern.compile(_Q + queryTablyName + "[\\s]+" + tAlias, Pattern.CASE_INSENSITIVE);
                        if (tAlias.equalsIgnoreCase(queryTablyName)
                                && !subQueryPattern.matcher(processedSql).find()) {
                            /**
                             * Эта проверка с финтом ушами нужна, т.к. даже в
                             * отсутствии алиаса, он все равно есть и равен
                             * queryTablyName. А так как алиас может в SQL
                             * совпадать с именем таблицы, то эти ситуации никак
                             * не различить, кроме как явной проверкой на
                             * нахождение такого алиаса и имени таблицы
                             * (подзапроса).
                             */
                            subQueryPattern = Pattern.compile(_Q + queryTablyName, Pattern.CASE_INSENSITIVE);
                        }
                    }
                    Matcher subQueryMatcher = subQueryPattern.matcher(processedSql);
                    if (subQueryMatcher.find()) {
                        SqlQuery subQuery = subQueriesProxy.getQuery(entity.getQueryName(), null, null, null);
                        if (subQuery != null && subQuery.getSqlText() != null) {
                            String subQueryText = subQuery.getSqlText();
                            subQueryText = replaceLinkedParameters(subQueryText, entity.getInRelations());

                            String sqlBegin = processedSql.substring(0, subQueryMatcher.start());
                            String sqlToInsert = " (" + subQueryText + ") ";
                            String sqlTail = processedSql.substring(subQueryMatcher.end());
                            if (tAlias != null && !tAlias.isEmpty()) {
                                processedSql = sqlBegin + sqlToInsert + " " + tAlias + " " + sqlTail;
                            } else {
                                processedSql = sqlBegin + sqlToInsert + " " + queryTablyName + " " + sqlTail;
                            }
                        }
                    }
                }
            }
            return processedSql;
        }
        return aSqlText;
    }

    private String replaceLinkedParameters(String aSqlText, Set<Relation<QueryEntity>> parametersRelations) {
        for (Relation<QueryEntity> rel : parametersRelations) {
            if (rel.getLeftEntity() instanceof QueryParametersEntity && rel.getLeftField() != null && rel.getRightParameter() != null) {
                aSqlText = Pattern.compile(COLON + rel.getRightParameter().getName() + "\\b", Pattern.CASE_INSENSITIVE).matcher(aSqlText).replaceAll(COLON + rel.getLeftField().getName());
            }
        }
        return aSqlText;
    }

    /**
     * @param aQuery
     * @return True if query is select query.
     * @throws Exception
     */
    public boolean fillFieldsFromMetadata(SqlQuery aQuery) throws Exception {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        try {
            Statement parsedQuery = parserManager.parse(new StringReader(aQuery.getSqlText()));
            if (parsedQuery instanceof Select) {
                Select select = (Select) parsedQuery;
                resolveOutputFieldsFromTables(aQuery, select.getSelectBody());
                return true;
            }
        } catch (JSQLParserException ex) {
            if (aQuery.isProcedure()) {
                Logger.getLogger(StoredQueryFactory.class.getName()).log(Level.WARNING, ex.getMessage());
            } else {
                throw ex;
            }
        }
        return false;
    }

    private void resolveOutputFieldsFromTables(SqlQuery aQuery, SelectBody aSelectBody) throws Exception {
        Map<String, FromItem> sources = SourcesFinder.getSourcesMap(SourcesFinder.TO_CASE.LOWER, aSelectBody);
        for (SelectItem sItem : ResultsFinder.getResults(aSelectBody)) {
            if (sItem instanceof AllColumns) {// *
                Map<String, FromItem> uniqueTables = prepareUniqueTables(sources);
                for (FromItem source : uniqueTables.values()) {
                    if (source instanceof Table) {
                        addTableFieldsToSelectResults(aQuery, (Table) source);
                    } else if (source instanceof SubSelect) {
                        Fields subFields = processSubQuery(aQuery, (SubSelect) source);
                        Fields destFields = aQuery.getFields();
                        subFields.toCollection().stream().forEach((field) -> {
                            destFields.add(field);
                        });
                    }
                }
            } else if (sItem instanceof AllTableColumns) {// t.*
                AllTableColumns cols = (AllTableColumns) sItem;
                assert cols.getTable() != null : "<table>.* syntax must lead to .getTable() != null";
                FromItem source = sources.get(cols.getTable().getWholeTableName().toLowerCase());
                if (source instanceof Table) {
                    addTableFieldsToSelectResults(aQuery, (Table) source);
                } else if (source instanceof SubSelect) {
                    Fields subFields = processSubQuery(aQuery, (SubSelect) source);
                    Fields destFields = aQuery.getFields();
                    subFields.toCollection().stream().forEach((field) -> {
                        destFields.add(field);
                    });
                }
            } else {
                assert sItem instanceof SelectExpressionItem;
                SelectExpressionItem col = (SelectExpressionItem) sItem;
                Field field = null;
                /* Если пользоваться этим приемом, то будет введение разработчика в заблуждение
                 * т.к. в дизайнере и автозавершении кода, поле результата будет поименовано
                 * так же как и поле-агрумент функции, а из скрипта оно будет недоступно.
                 if (col.getExpression() instanceof Function) {
                 Function func = (Function) col.getExpression();
                 if (func.getParameters() != null && func.getParameters().getExpressions() != null
                 && func.getParameters().getExpressions().size() == 1) {
                 Expression firstArg = (Expression) func.getParameters().getExpressions().get(0);
                 if (firstArg instanceof Column) {
                 field = resolveFieldByColumn(aQuery, (Column) firstArg, col, tables);
                 }
                 }
                 } else */
                if (col.getExpression() instanceof Column) {
                    field = resolveFieldByColumn(aQuery, (Column) col.getExpression(), col, sources);
                } else // free expression like a ...,'text' as txt,...
                {
                    field = null;
                    /*
                     * // Absent alias generation is parser's work. field = new
                     * Field(col.getAlias()); // Such field is absent in
                     * database tables and so, field's table is the processed
                     * query. field.setTableName(Constants.QUERY_ID_PREFIX
                     * + aQuery.getEntityId().toString()); /** There is an
                     * unsolved problem about type of the expression. This might
                     * be solved using manually setted up field's type and
                     * description information during
                     * "putStoredTableFieldsMetadata(...)" call.
                     */
                    //field.getTypeInfo().setSqlType(Types.OTHER);
                }
                if (field == null) {
                    // Absent alias generation is parser's work.
                    // Безымянные поля, получающиеся когда нет алиаса, будут
                    // замещены полями полученными из базы во время исполнения запроса.

                    field = new Field(col.getAlias() != null ? col.getAlias().getName()
                            : (col.getExpression() instanceof Column ? ((Column) col.getExpression()).getColumnName() : ""));

                    field.setTableName(aQuery.getEntityName());
                    /**
                     * There is an unsolved problem about type of the
                     * expression. This might be solved using manually setted up
                     * field's type and description information during
                     * "putStoredTableFieldsMetadata(...)" call.
                     */
                    field.setType(null);
                }
                aQuery.getFields().add(field);
            }
        }
    }

    protected class FieldsResult {

        public Fields fields;
        public boolean fromRealTable;

        public FieldsResult(Fields aResult, boolean aFromRealTable) {
            super();
            fields = aResult;
            fromRealTable = aFromRealTable;
        }
    }

    /**
     * Returns cached table fields if <code>aTablyName</code> is a table name or
     * query fields if <code>aTablyName</code> is query tably name in format:
     * #&lt;queryName&gt;.
     *
     * @param aDatasourceName Database identifier, the query belongs to. That
     *                        database is query-inner table metadata source, but query is stored in
     *                        application.
     * @param aTablyName      Table or query tably name.
     * @return Fields instance.
     * @throws Exception
     */
    protected FieldsResult getTablyFields(String aDatasourceName, String aTablyName) throws Exception {
        if (aTablyName.startsWith(Constants.STORED_QUERY_REF_PREFIX)) {
            // Reference to a stored subquery
            String queryName = aTablyName.substring(Constants.STORED_QUERY_REF_PREFIX.length());
            SqlQuery query = subQueriesProxy.getQuery(queryName, null, null, null);
            if (query != null) {
                return new FieldsResult(query.getFields(), false);
            } else {
                return null;
            }
        } else {
            // Reference to a table
            try {
                Fields tableFields = database.getMetadataCache(aDatasourceName).getTable(aTablyName);
                return new FieldsResult(tableFields, true);
            } catch (Exception ex) {
                return null;
            }
        }
    }

    private Field resolveFieldByColumn(SqlQuery aQuery, Column column, SelectExpressionItem selectItem, Map<String, FromItem> aSources) throws Exception {
        Field field = null;
        FromItem fieldSource = null;// Это таблица парсера - источник данных в составе запроса.
        boolean fieldFromRealTable = false;
        if (column.getTable() != null && column.getTable().getWholeTableName() != null) {
            FromItem namedSource = aSources.get(column.getTable().getWholeTableName().toLowerCase());
            if (namedSource != null) {
                if (namedSource instanceof Table) {
                    /**
                     * Таблица поля, предоставляемая парсером никак не связана с
                     * таблицей из списка from. Поэтому мы должны связать их
                     * самостоятельно. Такая вот особенность парсера.
                     */
                    FieldsResult tableFieldsResult = getTablyFields(aQuery.getDatasourceName(), ((Table) namedSource).getWholeTableName());
                    if (tableFieldsResult != null && tableFieldsResult.fields != null && tableFieldsResult.fields.contains(column.getColumnName())) {
                        field = tableFieldsResult.fields.get(column.getColumnName());
                        fieldSource = namedSource;
                        fieldFromRealTable = tableFieldsResult.fromRealTable;
                    }
                } else if (namedSource instanceof SubSelect) {
                    Fields subFields = processSubQuery(aQuery, (SubSelect) namedSource);
                    if (subFields.contains(column.getColumnName())) {
                        field = subFields.get(column.getColumnName());
                        fieldSource = namedSource;
                        fieldFromRealTable = false;
                    }
                }
            }
        }
        if (field == null) {
            /**
             * Часто бывает, что алиас/имя таблицы из которой берется поле не
             * указаны. Поэтому парсер не предоставляет таблицу. В этом случае
             * как и в первой версии поищем первую таблицу, содержащую поле с
             * таким именем.
             */
            for (FromItem anySource : aSources.values()) {
                if (anySource instanceof Table) {
                    FieldsResult fieldsResult = getTablyFields(aQuery.getDatasourceName(), ((Table) anySource).getWholeTableName());
                    if (fieldsResult != null && fieldsResult.fields != null && fieldsResult.fields.contains(column.getColumnName())) {
                        field = fieldsResult.fields.get(column.getColumnName());
                        fieldSource = anySource;
                        fieldFromRealTable = fieldsResult.fromRealTable;
                        break;
                    }
                } else if (anySource instanceof SubSelect) {
                    Fields fields = processSubQuery(aQuery, (SubSelect) anySource);
                    if (fields != null && fields.contains(column.getColumnName())) {
                        field = fields.get(column.getColumnName());
                        fieldSource = anySource;
                        fieldFromRealTable = false;
                        break;
                    }
                }
            }
        }
        if (field != null) {
            /**
             * Скопируем поле, чтобы избежать пересечения информации о полях
             * таблицы из-за её участия в разных запросах.
             */
            Field copied = new Field();
            copied.assignFrom(field);
            if (fieldFromRealTable) {
                TypesResolver resolver = database.getMetadataCache(aQuery.getDatasourceName()).getDataSourceSqlDriver().getTypesResolver();
                JdbcColumn jField = (JdbcColumn) field;
                copied.setType(resolver.toApplicationType(jField.getJdbcType(), jField.getType()));
                if (jField.getSchemaName() != null && !jField.getSchemaName().isEmpty()) {
                    copied.setTableName(jField.getSchemaName() + "." + copied.getTableName());
                }
            }
            /**
             * Заменим имя поля из оригинальной таблицы на алиас. Если его нет,
             * то его надо сгенерировать. Генерация алиаса, - это работа
             * парсера. По возможности, парсер должен генерировать алиасы
             * совпадающие с именем поля.
             */
            copied.setName(selectItem.getAlias() != null ? selectItem.getAlias().getName() : column.getColumnName());
            copied.setOriginalName(column.getColumnName() != null ? column.getColumnName() : copied.getName());
            /**
             * Заменять имя оригинальной таблицы нельзя, особенно если это поле
             * ключевое т.к. при установлении связи по этим полям будут
             * проблемы. ORM-у и дизайнеру придется "разматывать" источник поля
             * сквозь все запросы до таблицы чтобы восстановить связи по ключам.
             * Здесь это делается исключительно ради очень специального
             * использования фабрики в дизайнере запросов.
             */
            if (aliasesToTableNames
                    && fieldSource != null && fieldSource.getAlias() != null && !fieldSource.getAlias().getName().isEmpty()) {
                copied.setTableName(fieldSource.getAlias().getName());
            }
            return copied;
        } else {
            return null;
        }
    }
}
