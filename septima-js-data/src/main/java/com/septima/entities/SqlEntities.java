package com.septima.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.septima.Database;
import com.septima.GenericType;
import com.septima.Metadata;
import com.septima.metadata.Parameter;
import com.septima.changes.EntityAction;
import com.septima.dataflow.DataProvider;
import com.septima.dataflow.EntityActionsBinder;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.EntityField;
import com.septima.metadata.ForeignKey;
import com.septima.metadata.JdbcColumn;
import com.septima.queries.CaseInsensitiveMap;
import com.septima.queries.CaseInsensitiveSet;
import com.septima.queries.ExtractParameters;
import com.septima.queries.InlineEntities;
import com.septima.sqldrivers.resolvers.TypesResolver;
import net.sf.jsqlparser.JSqlParser;
import net.sf.jsqlparser.JSqlParserException;
import net.sf.jsqlparser.SeptimaSqlParser;
import net.sf.jsqlparser.UncheckedJSqlParserException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.syntax.FromItems;
import net.sf.jsqlparser.syntax.SelectItems;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

/**
 * @author mg
 */
public class SqlEntities {

    private static final String ENTITY_NAME_MISSING_MSG = "Entity name missing.";
    private static final String LOADING_QUERY_MSG = "Loading entity '%s'.";
    private static final String LOADED_QUERY_MSG = "Entity '%s' loaded.";

    private final Path applicationPath;
    private final String defaultDataSource;
    private final Map<String, SqlEntity> entities = new ConcurrentHashMap<>();
    private final Executor jdbcPerformer;
    private final Executor futuresExecutor;

    private final Map<String, Database> databases = new ConcurrentHashMap<>();

    public SqlEntities(Path anApplicationPath, String aDefaultDataSource) {
        this(anApplicationPath, aDefaultDataSource, Database.jdbcTasksPerformer(32), ForkJoinPool.commonPool());
    }

    public SqlEntities(Path anApplicationPath, String aDefaultDataSource, Executor aJdbcPerformer, Executor aFuturesExecutor) {
        super();
        Objects.requireNonNull(anApplicationPath, "aApplicationPath is required argument");
        Objects.requireNonNull(aDefaultDataSource, "aDefaultDataSource is required argument");
        Objects.requireNonNull(aJdbcPerformer, "aJdbcPerformer is required argument");
        Objects.requireNonNull(aFuturesExecutor, "aFuturesExecutor is required argument");
        applicationPath = anApplicationPath.normalize();
        defaultDataSource = aDefaultDataSource;
        jdbcPerformer = aJdbcPerformer;
        futuresExecutor = aFuturesExecutor;
    }

    public Path getApplicationPath() {
        return applicationPath;
    }

    public String getDefaultDataSource() {
        return defaultDataSource;
    }

    public SqlEntity loadEntity(String anEntityName) {
        return loadEntity(anEntityName, new HashSet<>());
    }

    /**
     * Ignores possible races while caching, because only {@code entities.put()} operation is present and
     * random extra put operations are not harmful.
     * Warning! Don't use {@code entities.computeIfAbsent()} or {@code entities.putIfAbsent()} while caching here because of recursive nature of loadEntity()
     *
     * @param anEntityName       Entity name as {@code #full/path} followed by hash sign, relative to application root path.
     * @param aIllegalReferences A {@link Set} with already processed entities names. Used to avoid cyclic references.
     * @return {@link SqlEntity} instance with inlined sub entities' sql text and substituted parameters names.
     * @throws SqlEntityCyclicReferenceException If entity body has a cyclic reference.
     * @see ConcurrentHashMap
     */
    public SqlEntity loadEntity(String anEntityName, Set<String> aIllegalReferences) throws SqlEntityCyclicReferenceException {
        Objects.requireNonNull(anEntityName, ENTITY_NAME_MISSING_MSG);
        if (aIllegalReferences.contains(anEntityName)) {
            throw new SqlEntityCyclicReferenceException(anEntityName);
        } else {
            aIllegalReferences.add(anEntityName);
        }
        try {
            if (entities.containsKey(anEntityName)) {
                return entities.get(anEntityName);
            } else {
                try {
                    Logger.getLogger(SqlEntities.class.getName()).finer(String.format(LOADING_QUERY_MSG, anEntityName));
                    String entityJsonFileName = anEntityName + ".sql.json";
                    File entityJsonFile = applicationPath.resolve(entityJsonFileName).toFile();
                    SqlEntity entity = constructEntity(anEntityName, readEntitySql(anEntityName), entityJsonFile, aIllegalReferences);
                    Logger.getLogger(SqlEntities.class.getName()).finer(String.format(LOADED_QUERY_MSG, anEntityName));
                    entities.put(anEntityName, entity);
                    return entity;
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                } catch (JSqlParserException ex) {
                    throw new UncheckedJSqlParserException(ex);
                } catch (SQLException ex) {
                    throw new UncheckedSQLException(ex);
                }
            }
        } finally {
            aIllegalReferences.remove(anEntityName);
        }
    }

    public Map<Database, List<EntityActionsBinder.BoundStatement>> bindChanges(List<EntityAction> aChangeLog) {
        Map<Database, List<EntityActionsBinder.BoundStatement>> bound = new HashMap<>();
        for (EntityAction change : aChangeLog) {
            SqlEntity entity = loadEntity(change.getEntityName());
            EntityActionsBinder binder = new EntityActionsBinder(entity);
            change.accept(binder);
            List<EntityActionsBinder.BoundStatement> boundToDatabase = bound.computeIfAbsent(entity.getDatabase(), d -> new ArrayList<>());
            boundToDatabase.addAll(binder.getLogEntries());
        }
        return Collections.unmodifiableMap(bound);
    }

    private Map<String, JdbcColumn> resolveTableColumns(Database database, Table aTable) throws SQLException {
        return database.getMetadata()
                .getTableColumns(aTable.getWholeTableName())
                .orElse(Map.of());
    }

    private static Map<String, EntityField> columnsToApplicationFields(Map<String, JdbcColumn> tableColumns, TypesResolver typesResolver) {
        return tableColumns.values().stream()
                .map(column -> new EntityField(
                                column.getName(),
                                column.getDescription(),
                                column.getOriginalName(),
                                column.getTableName(),
                                typesResolver.toGenericType(column.getJdbcType(), column.getRdbmsType()),
                                column.isNullable(),
                                column.isPk(),
                                column.getFk()
                        )
                )
                .collect(
                        () -> new CaseInsensitiveMap<>(new LinkedHashMap<>()),
                        (r, field) -> r.put(field.getName(), field),
                        Map::putAll
                );
    }

    private String readEntitySql(String aEntityName) throws IOException {
        Objects.requireNonNull(aEntityName, ENTITY_NAME_MISSING_MSG);
        String entitySqlFileName = aEntityName + ".sql";
        File mainQueryFile = applicationPath.resolve(entitySqlFileName).toFile();
        if (mainQueryFile.exists()) {
            if (!mainQueryFile.isDirectory()) {
                return new String(Files.readAllBytes(mainQueryFile.toPath()), StandardCharsets.UTF_8);
            } else {
                throw new FileNotFoundException(entitySqlFileName + "' at path: " + applicationPath + " is a directory.");
            }
        } else {
            throw new FileNotFoundException("Can't find '" + entitySqlFileName + "' from path: " + applicationPath);
        }
    }

    private static JsonNode parseEntitySettings(File jsonFile) throws IOException {
        if (jsonFile.exists() && !jsonFile.isDirectory()) {
            String json = new String(Files.readAllBytes(jsonFile.toPath()), StandardCharsets.UTF_8);
            ObjectMapper jsonMapper = new ObjectMapper();
            return jsonMapper.readTree(json);
        } else {
            return null;
        }
    }

    private static Consumer<? super Map.Entry<String, JsonNode>> parameterReader(Map<String, Parameter> params, Map<String, Map<String, String>> paramsBinds) {
        return (parameterEntry) -> {
            String parameterName = parameterEntry.getKey();
            Parameter parameter = params.getOrDefault(parameterName, new Parameter(parameterName));
            JsonNode parameterNode = parameterEntry.getValue();
            JsonNode typeNode = parameterNode.get("type");
            GenericType type = typeNode != null && typeNode.isTextual() ? GenericType.of(typeNode.asText(), parameter.getType()) : parameter.getType();
            JsonNode descriptionNode = parameterNode.get("description");
            String description = descriptionNode != null && descriptionNode.isTextual() ? descriptionNode.asText(parameter.getDescription()) : parameter.getDescription();
            JsonNode valueNode = parameterNode.get("value");
            String value = valueNode != null && valueNode.isTextual() ? valueNode.asText() : null;
            JsonNode outNode = parameterNode.get("out");
            boolean out = outNode != null && outNode.isBoolean() && outNode.asBoolean();
            params.put(parameterName, new Parameter(
                    parameterName,
                    value,
                    type,
                    out ? Parameter.Mode.InOut : Parameter.Mode.In,
                    description
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
                                        Map<String, String> subToOuterParams = paramsBinds.computeIfAbsent(subQueryName, sqn -> new HashMap<>());
                                        subToOuterParams.put(subQueryParamNode.asText(), parameterName);
                                    }
                                }
                            }
                        });
            }
        };
    }

    private static Consumer<Map.Entry<String, JsonNode>> fieldReader(Map<String, EntityField> fields, String entityName) {
        return (fieldEntry) -> {
            String fieldName = fieldEntry.getKey();
            EntityField entityField = fields.getOrDefault(fieldName, new EntityField(fieldName));
            JsonNode fieldNode = fieldEntry.getValue();
            JsonNode typeNode = fieldNode.get("type");
            GenericType type = typeNode != null && typeNode.isTextual() ? GenericType.of(typeNode.asText(), entityField.getType()) : entityField.getType();
            JsonNode descriptionNode = fieldNode.get("description");
            String description = descriptionNode != null && descriptionNode.isTextual() ? descriptionNode.asText(entityField.getDescription()) : entityField.getDescription();
            JsonNode nullableNode = fieldNode.get("nullable");
            boolean nullable = nullableNode != null && nullableNode.isBoolean() ? nullableNode.asBoolean() : entityField.isNullable();
            JsonNode originalNameNode = fieldNode.get("originalName");
            String originalName = originalNameNode != null && originalNameNode.isTextual() ? originalNameNode.asText(entityField.getOriginalName()) : entityField.getOriginalName();
            JsonNode tableNameNode = fieldNode.get("tableName");
            String tableName = tableNameNode != null && tableNameNode.isTextual() ? tableNameNode.asText(entityField.getTableName()) : entityField.getTableName();
            JsonNode keyNode = fieldNode.get("key");
            boolean key = keyNode != null && keyNode.isBoolean() ? keyNode.asBoolean() : entityField.isPk();
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
            fields.put(fieldName, new EntityField(
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
                                            null, entityName, fieldName, null,
                                            ForeignKey.ForeignKeyRule.NO_ACTION, ForeignKey.ForeignKeyRule.NO_ACTION, false,
                                            null, referencedEntity, referencedKey, null)
                                    : entityField.getFk()
                    )
            );
        };
    }

    private static Set<String> jsonStringArrayToSet(JsonNode aNode) {
        Set<String> read = new HashSet<>();
        if (aNode != null && aNode.isArray()) {
            for (int i = 0; i < aNode.size(); i++) {
                JsonNode writableTableNode = aNode.get(i);
                if (writableTableNode.isTextual()) {
                    String writableTable = writableTableNode.asText();
                    if (writableTable != null && !writableTable.isEmpty()) {
                        read.add(writableTable);
                    }
                }
            }
        }
        return read;
    }

    private SqlEntity constructEntity(String anEntityName, String anEntitySql, File anEntityJsonFile, Set<String> aIllegalRefrences) throws IOException, JSqlParserException, SQLException {
        Objects.requireNonNull(anEntityName, ENTITY_NAME_MISSING_MSG);
        Objects.requireNonNull(anEntitySql, "anEntitySql is required argument");
        Objects.requireNonNull(anEntityJsonFile, "anEntityJsonFile is required argument");

        if (!anEntitySql.isEmpty()) {
            JsonNode entityDocument = parseEntitySettings(anEntityJsonFile);

            JsonNode dataSourceNode = entityDocument != null ? entityDocument.get("source") : null;
            String dataSource = dataSourceNode != null && dataSourceNode.isTextual() ? dataSourceNode.asText() : defaultDataSource;
            Database database = databaseOf(dataSource);
            JsonNode procedureNode = entityDocument != null ? entityDocument.get("procedure") : null;
            boolean procedure = procedureNode != null && procedureNode.asBoolean();

            JsonNode titleNode = entityDocument != null ? entityDocument.get("title") : null;
            String title = titleNode != null && titleNode.isTextual() ? titleNode.asText() : anEntityName;
            JsonNode classNameNode = entityDocument != null ? entityDocument.get("className") : null;
            String className = classNameNode != null && classNameNode.isTextual() ? classNameNode.asText() : null;
            JsonNode sqlNode = entityDocument != null ? entityDocument.get("sql") : null;
            String customSql = sqlNode != null && sqlNode.isTextual() ? sqlNode.asText() : null;
            JsonNode readonlyNode = entityDocument != null ? entityDocument.get("readonly") : null;
            boolean readonly = readonlyNode != null && readonlyNode.asBoolean();
            JsonNode publicNode = entityDocument != null ? entityDocument.get("public") : null;
            boolean publicAccess = publicNode != null && publicNode.asBoolean();
            JsonNode pageSizeNode = entityDocument != null ? entityDocument.get("pageSize") : null;
            int pageSize = pageSizeNode != null && pageSizeNode.isInt() ? pageSizeNode.asInt(DataProvider.NO_PAGING_PAGE_SIZE) : DataProvider.NO_PAGING_PAGE_SIZE;

            JSqlParser sqlParser = new SeptimaSqlParser();
            Statement querySyntax = sqlParser.parse(new StringReader(anEntitySql));

            boolean command = !(querySyntax instanceof Select);
            JsonNode commandNode = entityDocument != null ? entityDocument.get("command") : null;
            command = commandNode != null && commandNode.isBoolean() ? commandNode.asBoolean() : command;

            Map<String, Parameter> params = ExtractParameters.from(querySyntax);
            // subQueryName, subQueryParameterName, parameterName
            Map<String, Map<String, String>> paramsBinds = new HashMap<>();
            JsonNode paramsNode = entityDocument != null ? entityDocument.get("parameters") : null;
            if (paramsNode != null && paramsNode.isObject()) {
                paramsNode.fields().forEachRemaining(parameterReader(params, paramsBinds));
            }

            InlineEntities.to(querySyntax, this, paramsBinds, anEntityJsonFile.getParentFile().toPath(), aIllegalRefrences);
            String sqlWithSubQueries = StatementDeParser.assemble(querySyntax);
            Map<String, EntityField> fields = columnsToApplicationFields(
                    resolveColumnsBySyntax(database, querySyntax), database.getSqlDriver().getTypesResolver()
            );

            Set<String> writable = new CaseInsensitiveSet(new HashSet<>());
            Set<String> readRoles = new HashSet<>();
            Set<String> writeRoles = new HashSet<>();

            if (entityDocument != null) {
                JsonNode fieldsNode = entityDocument.get("fields");
                if (fieldsNode != null) {
                    fieldsNode.fields().forEachRemaining(fieldReader(fields, anEntityName));
                }
                JsonNode writableNode = entityDocument.get("writable");
                writable.addAll(jsonStringArrayToSet(writableNode));
                JsonNode rolesNode = entityDocument.get("roles");
                if (rolesNode != null && rolesNode.isObject()) {
                    JsonNode readRolesNode = rolesNode.get("read");
                    readRoles.addAll(jsonStringArrayToSet(readRolesNode));
                    JsonNode writeRolesNode = rolesNode.get("write");
                    writeRoles.addAll(jsonStringArrayToSet(writeRolesNode));
                }
            }
            return new SqlEntity(
                    database,
                    sqlWithSubQueries,
                    customSql,
                    anEntityName,
                    className,
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
            throw new IllegalStateException("'" + anEntityName + "' has an empty sql text.");
        }
    }

    private Map<String, JdbcColumn> resolveColumnsBySyntax(Database database, Statement parsedQuery) throws SQLException {
        if (parsedQuery instanceof Select) {
            Select select = (Select) parsedQuery;
            return resolveOutputColumnsFromSources(database, select.getSelectBody());
        } else {
            return Map.of();
        }
    }

    private Map<String, JdbcColumn> resolveOutputColumnsFromSources(Database database, SelectBody aSelectBody) throws SQLException {
        Map<String, JdbcColumn> fields = new CaseInsensitiveMap<>(new LinkedHashMap<>());
        Map<String, FromItem> sources = FromItems.find(FromItems.ToCase.LOWER, aSelectBody);
        for (SelectItem selectItem : SelectItems.find(aSelectBody)) {
            if (selectItem instanceof AllColumns) {// *
                for (FromItem source : sources.values()) {
                    if (source instanceof Table) {
                        Map<String, JdbcColumn> tableColumns = resolveTableColumns(database, (Table) source);
                        fields.putAll(tableColumns);
                    } else if (source instanceof SubSelect) {
                        Map<String, JdbcColumn> subSelectColumns = resolveOutputColumnsFromSources(database, ((SubSelect) source).getSelectBody());
                        fields.putAll(subSelectColumns);
                    }
                }
            } else if (selectItem instanceof AllTableColumns) {// t.*
                AllTableColumns cols = (AllTableColumns) selectItem;
                assert cols.getTable() != null : "<table>.* syntax must lead to .getTable() != null";
                FromItem source = sources.get(cols.getTable().getWholeTableName().toLowerCase());
                if (source instanceof Table) {
                    Map<String, JdbcColumn> tableColumns = resolveTableColumns(database, (Table) source);
                    fields.putAll(tableColumns);
                } else if (source instanceof SubSelect) {
                    // In case of sub query, cols.getTable() returns surrogate table, containing only sub query's alias as table name.
                    Map<String, JdbcColumn> subSelectColumns = resolveOutputColumnsFromSources(database, ((SubSelect) source).getSelectBody());
                    fields.putAll(subSelectColumns);
                }
            } else {
                assert selectItem instanceof SelectExpressionItem;
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                if (selectExpressionItem.getExpression() instanceof Column) {
                    JdbcColumn jdbcColumn = resolveJdbcColumnByColumnExpression(database, (Column) selectExpressionItem.getExpression(), selectExpressionItem.getAliasName(), sources);
                    fields.put(jdbcColumn.getName(), jdbcColumn);
                } else {
                    /*
                     * free expression like a ...,'text' as txt,...
                     */
                    if (!selectExpressionItem.getAliasName().isEmpty()) {
                        JdbcColumn jdbcColumn = new JdbcColumn(selectExpressionItem.getAliasName());
                        fields.put(jdbcColumn.getName(), jdbcColumn);
                    } //else {
                    // Unnamed expression columns will be replaced by fact fields during data receiving from a database
                    //}
                }
            }
        }
        return fields;
    }

    private static JdbcColumn asAliasedColumn(JdbcColumn resolved, Column column, String alias) {
        return new JdbcColumn(
                alias != null && !alias.isEmpty() ? alias : column.getColumnName(),
                resolved.getDescription(),
                resolved.getOriginalName(),
                resolved.getTableName(),
                resolved.getRdbmsType(),
                resolved.isNullable(),
                resolved.isPk(),
                resolved.getFk(),
                resolved.getSize(),
                resolved.getScale(),
                resolved.getPrecision(),
                resolved.isSigned(),
                resolved.getSchema(),
                resolved.getJdbcType()
        );
    }

    private JdbcColumn resolveColumnBySource(Database database, Column column, String alias, FromItem source) throws SQLException {
        if (source instanceof Table) {
            Map<String, JdbcColumn> tableFields = resolveTableColumns(database, (Table) source);
            JdbcColumn resolved = tableFields.getOrDefault(column.getColumnName(), new JdbcColumn(column.getColumnName()));
            return asAliasedColumn(resolved, column, alias);
        } else if (source instanceof SubSelect) {
            Map<String, JdbcColumn> subFields = resolveOutputColumnsFromSources(database, ((SubSelect) source).getSelectBody());
            JdbcColumn resolved = subFields.getOrDefault(column.getColumnName(), new JdbcColumn(column.getColumnName()));
            return asAliasedColumn(resolved, column, alias);
        } else {
            return new JdbcColumn(column.getColumnName());
        }
    }

    private boolean isFieldInSource(Database database, Column column, FromItem source) throws SQLException {
        if (source instanceof Table) {
            Map<String, JdbcColumn> tableFields = resolveTableColumns(database, (Table) source);
            return tableFields.containsKey(column.getColumnName());
        } else if (source instanceof SubSelect) {
            Map<String, JdbcColumn> subFields = resolveOutputColumnsFromSources(database, ((SubSelect) source).getSelectBody());
            return subFields.containsKey(column.getColumnName());
        } else {
            return false;
        }
    }

    private JdbcColumn resolveJdbcColumnByColumnExpression(Database database, Column column, String alias, Map<String, FromItem> aSourcesByAlias) throws SQLException {
        if (column.getTable() != null &&
                column.getTable().getWholeTableName() != null &&
                !column.getTable().getWholeTableName().isEmpty()) {
            /*
             * Таблица поля, предоставляемая парсером никак не связана с
             * таблицей из списка from. Поэтому мы должны связать их
             * самостоятельно.
             */
            FromItem source = aSourcesByAlias.get(column.getTable().getWholeTableName().toLowerCase());
            return resolveColumnBySource(database, column, alias, source);
        } else {
            /*
             * Часто бывает, что таблица из которого берется поле не указана.
             * Поэтому парсер не предоставляет таблицу.
             * В этом случае поищем первую таблицу, содержащую поле с таким именем.
             * Замечание: Таблица и подзапрос для парсера это одно и то же.
             * Замечание: Таблица или подзапрос может быть указан как имя настоящей таблицы или как алиас таблицы или подзапроса.
             */
            return aSourcesByAlias.values().stream()
                    .dropWhile(source -> {
                        try {
                            return !isFieldInSource(database, column, source);
                        } catch (SQLException ex) {
                            throw new UncheckedSQLException(ex);
                        }
                    })
                    .findFirst()
                    .map(source -> {
                        try {
                            return resolveColumnBySource(database, column, alias, source);
                        } catch (SQLException ex) {
                            throw new UncheckedSQLException(ex);
                        }
                    })
                    .orElse(new JdbcColumn(alias != null && !alias.isEmpty() ? alias : column.getColumnName()));
        }
    }

    private Database databaseOf(final String aDataSourceName) {
        Objects.requireNonNull(aDataSourceName, "aDataSourceName ia required argument");
        return databases.computeIfAbsent(aDataSourceName, dsn -> {
            try {
                DataSource ds = Database.obtainDataSource(aDataSourceName);
                Metadata metadata = Metadata.of(ds);
                return new Database(
                        ds,
                        metadata,
                        jdbcPerformer,
                        futuresExecutor
                );
            } catch (NamingException ex) {
                throw new IllegalStateException(ex);
            } catch (SQLException ex) {
                throw new UncheckedSQLException(ex);
            }
        });
    }
}
