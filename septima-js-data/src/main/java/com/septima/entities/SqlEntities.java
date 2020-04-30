package com.septima.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.septima.Database;
import com.septima.GenericType;
import com.septima.Metadata;
import com.septima.changes.EntityAction;
import com.septima.dataflow.DataProvider;
import com.septima.dataflow.EntityActionsBinder;
import com.septima.jdbc.DataSources;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.EntityField;
import com.septima.metadata.ForeignKey;
import com.septima.metadata.JdbcColumn;
import com.septima.metadata.Parameter;
import com.septima.queries.CaseInsensitiveMap;
import com.septima.queries.CaseInsensitiveSet;
import com.septima.queries.ExtractParameters;
import com.septima.queries.InlineEntities;
import com.septima.queries.SqlQuery;
import com.septima.sqldrivers.SqlDriver;
import net.sf.jsqlparser.JSqlParserException;
import net.sf.jsqlparser.SeptimaSqlParser;
import net.sf.jsqlparser.UncheckedJSqlParserException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.syntax.FromItems;
import net.sf.jsqlparser.syntax.SelectItems;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author mg
 */
public class SqlEntities {

    private static final String ENTITY_NAME_MISSING_MSG = "Entity name missing.";
    private static final String LOADING_QUERY_MSG = "Loading entity '%s'.";
    private static final String LOADED_QUERY_MSG = "Entity '%s' loaded.";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final boolean compileEntities;
    private final Path entitiesRoot;
    private final Path resourcesEntitiesRoot;
    private final String defaultDataSource;
    private final Map<String, SqlEntity> entities = new ConcurrentHashMap<>();
    private final Map<String, SqlQuery> queries = new ConcurrentHashMap<>();
    private final Executor jdbcPerformer;
    private final Executor futuresExecutor;

    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<Database, String> dataSources = new ConcurrentHashMap<>();

    public SqlEntities(Path anEntitiesRoot, String aDefaultDataSource, boolean aCompileEntities) {
        this(anEntitiesRoot, aDefaultDataSource, Database.jdbcTasksPerformer(32), ForkJoinPool.commonPool(), aCompileEntities);
    }

    public SqlEntities(Path anEntitiesRoot, String aDefaultDataSource, Executor aJdbcPerformer, Executor aFuturesExecutor, boolean aCompileEntities) {
        this(null, anEntitiesRoot, aDefaultDataSource, aJdbcPerformer, aFuturesExecutor, aCompileEntities);
    }

    public SqlEntities(Path aResourcesEntitiesRoot, Path anEntitiesRoot, String aDefaultDataSource, Executor aJdbcPerformer, Executor aFuturesExecutor, boolean aCompileEntities) {
        super();
        Objects.requireNonNull(
                Objects.requireNonNullElse(aResourcesEntitiesRoot, anEntitiesRoot),
                "One of ['aResourcesEntitiesRoot', 'anEntitiesRoot'] is required argument"
        );
        Objects.requireNonNull(aDefaultDataSource, "aDefaultDataSource is required argument");
        Objects.requireNonNull(aJdbcPerformer, "aJdbcPerformer is required argument");
        Objects.requireNonNull(aFuturesExecutor, "aFuturesExecutor is required argument");
        entitiesRoot = anEntitiesRoot != null ? anEntitiesRoot.normalize() : null;
        resourcesEntitiesRoot = aResourcesEntitiesRoot != null ? aResourcesEntitiesRoot.normalize() : null;
        defaultDataSource = aDefaultDataSource;
        jdbcPerformer = aJdbcPerformer;
        futuresExecutor = aFuturesExecutor;
        compileEntities = aCompileEntities;
    }

    private static Map<String, EntityField> columnsToApplicationFields(Map<String, JdbcColumn> tableColumns, SqlDriver aDriver) {
        return tableColumns.values().stream()
                .map(column -> new EntityField(
                                aDriver.unescapeNameIfNeeded(column.getName()),
                                column.getDescription(),
                                column.getOriginalName(),
                                column.getTableName(),
                                aDriver.getTypesResolver().toGenericType(column.getJdbcType(), column.getRdbmsType()),
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

    private static Consumer<? super Map.Entry<String, JsonNode>> parameterReader(Map<String, Parameter> params, Map<String, Map<String, String>> paramsBinds) {
        return (parameterEntry) -> {
            String parameterName = parameterEntry.getKey();
            Parameter parameter = params.getOrDefault(parameterName, new Parameter(parameterName));
            JsonNode parameterNode = parameterEntry.getValue();
            JsonNode typeNode = parameterNode.get("type");
            GenericType type = typeNode != null && typeNode.isTextual() ? GenericType.of(typeNode.asText(), parameter.getType()) : parameter.getType();
            JsonNode subTypeNode = parameterNode.get("subType");
            String subType = subTypeNode != null && subTypeNode.isTextual() ? subTypeNode.asText() : null;
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
                    subType,
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
            EntityField basicField = fields.getOrDefault(fieldName, new EntityField(fieldName));
            JsonNode fieldNode = fieldEntry.getValue();
            JsonNode typeNode = fieldNode.get("type");
            GenericType type = typeNode != null && typeNode.isTextual() ? GenericType.of(typeNode.asText(), basicField.getType()) : basicField.getType();
            JsonNode subTypeNode = fieldNode.get("subType");
            String subType = subTypeNode != null && subTypeNode.isTextual() ? subTypeNode.asText() : null;
            JsonNode descriptionNode = fieldNode.get("description");
            String description = descriptionNode != null && descriptionNode.isTextual() ? descriptionNode.asText(basicField.getDescription()) : basicField.getDescription();
            JsonNode nullableNode = fieldNode.get("nullable");
            boolean nullable = nullableNode != null && nullableNode.isBoolean() ? nullableNode.asBoolean() : basicField.isNullable();
            JsonNode originalNameNode = fieldNode.get("originalName");
            String originalName = originalNameNode != null && originalNameNode.isTextual() ? originalNameNode.asText(basicField.getOriginalName()) : basicField.getOriginalName();
            JsonNode tableNameNode = fieldNode.get("tableName");
            String tableName = tableNameNode != null && tableNameNode.isTextual() ? tableNameNode.asText(basicField.getTableName()) : basicField.getTableName();
            JsonNode keyNode = fieldNode.get("key");
            boolean key = keyNode != null && keyNode.isBoolean() ? keyNode.asBoolean() : basicField.isPk();
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
                            subType,
                            nullable,
                            key,
                            referencedEntity != null && !referencedEntity.isEmpty() &&
                                    referencedKey != null && !referencedKey.isEmpty() ?
                                    new ForeignKey(
                                            null, entityName, fieldName, null,
                                            ForeignKey.ForeignKeyRule.NO_ACTION, ForeignKey.ForeignKeyRule.NO_ACTION, false,
                                            null, referencedEntity, referencedKey, null)
                                    : basicField.getFk()
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

    public Path getEntitiesRoot() {
        return entitiesRoot;
    }

    public Path getResourcesEntitiesRoot() {
        return resourcesEntitiesRoot;
    }

    public String getDefaultDataSource() {
        return defaultDataSource;
    }

    public String dataSourceOf(Database aDatabase) {
        return dataSources.get(aDatabase);
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
                    Path startOfReferences = entitiesRoot != null ? entitiesRoot.resolve(anEntityName).getParent() : resourcesEntitiesRoot.resolve(anEntityName).getParent();
                    SqlEntity entity = constructEntity(anEntityName, readEntitySql(anEntityName), readEntityJson(anEntityName), startOfReferences, aIllegalReferences);
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

    public SqlQuery loadQuery(String anEntityName) {
        return queries.computeIfAbsent(anEntityName, entityName -> loadEntity(entityName).toQuery());
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

    private String resolveResourceName(String relative) {
        return resourcesEntitiesRoot.resolve(relative).toString().replace(File.separatorChar, '/');
    }

    public boolean exists(String aEntityName) {
        Objects.requireNonNull(aEntityName, ENTITY_NAME_MISSING_MSG);
        String entitySqlFileName = aEntityName + ".sql";
        if (entitiesRoot != null) {
            File mainQueryFile = entitiesRoot.resolve(entitySqlFileName).toFile();
            return mainQueryFile.exists();
        } else {
            URL resourceUrl = SqlEntities.class.getResource(resolveResourceName(entitySqlFileName));
            return resourceUrl != null;
        }
    }

    private String readEntitySql(String aEntityName) throws IOException {
        Objects.requireNonNull(aEntityName, ENTITY_NAME_MISSING_MSG);
        String entitySqlFileName = aEntityName + ".sql";
        if (entitiesRoot != null) {
            File mainQueryFile = entitiesRoot.resolve(entitySqlFileName).toFile();
            if (mainQueryFile.exists()) {
                if (!mainQueryFile.isDirectory()) {
                    return Files.readString(mainQueryFile.toPath(), StandardCharsets.UTF_8);
                } else {
                    throw new FileNotFoundException(entitySqlFileName + "' at path: " + entitiesRoot + " is a directory.");
                }
            } else if (compileEntities) {
                throw new FileNotFoundException("Can't find '" + entitySqlFileName + "' from path: " + entitiesRoot);
            } else {
                return null;
            }
        } else {
            URL resourceUrl = SqlEntities.class.getResource(resolveResourceName(entitySqlFileName));
            if (resourceUrl != null) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(SqlEntities.class.getResourceAsStream(resolveResourceName(entitySqlFileName)), StandardCharsets.UTF_8))) {
                    return in.lines().collect(Collectors.joining("\n", "", "\n"));
                }
            } else if (compileEntities) {
                throw new FileNotFoundException("Can't find '" + entitySqlFileName + "' resource from resource path: " + resourcesEntitiesRoot.toString().replace(File.separatorChar, '/'));
            } else {
                return null;
            }
        }
    }

    private String readEntityJson(String anEntityName) throws IOException {
        Objects.requireNonNull(anEntityName, ENTITY_NAME_MISSING_MSG);
        String entityJsonFileName = anEntityName + ".sql.json";
        if (entitiesRoot != null) {
            File jsonFile = entitiesRoot.resolve(entityJsonFileName).toFile();
            if (jsonFile.exists()) {
                if (!jsonFile.isDirectory()) {
                    return Files.readString(jsonFile.toPath(), StandardCharsets.UTF_8);
                } else {
                    throw new FileNotFoundException(entityJsonFileName + "' at path: " + entitiesRoot + " is a directory.");
                }
            } else if (!compileEntities) {
                throw new FileNotFoundException("Can't find '" + entityJsonFileName + "' from path: " + entitiesRoot);
            } else {
                return null;
            }
        } else {
            URL resourceUrl = SqlEntities.class.getResource(resolveResourceName(entityJsonFileName));
            if (resourceUrl != null) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(SqlEntities.class.getResourceAsStream(resolveResourceName(entityJsonFileName)), StandardCharsets.UTF_8))) {
                    return in.lines().collect(Collectors.joining("\n", "", "\n"));
                }
            } else if (!compileEntities) {
                throw new FileNotFoundException("Can't find '" + entityJsonFileName + "' resource from resource path: " + resourcesEntitiesRoot.toString().replace(File.separatorChar, '/'));
            } else {
                return null;
            }
        }
    }

    private SqlEntity constructEntity(String anEntityName, String anEntitySql, String anEntityJson, Path aStartOfReferences, Set<String> aIllegalReferences) throws IOException, JSqlParserException, SQLException {
        Objects.requireNonNull(anEntityName, ENTITY_NAME_MISSING_MSG);
        if (compileEntities) {
            Objects.requireNonNull(anEntitySql, "anEntitySql is required argument");
            if (anEntitySql.isBlank()) {
                throw new IllegalArgumentException("anEntitySql should not be blank");
            }
        } else {
            Objects.requireNonNull(anEntityJson, "anEntityJson is required argument if no entities compilation is considered");
            if (anEntityJson.isBlank()) {
                throw new IllegalArgumentException("anEntityJson should not be blank");
            }
        }

        JsonNode entityDocument = anEntityJson != null ? JSON.readTree(anEntityJson) : null;

        JsonNode dataSourceNode = entityDocument != null ? entityDocument.get("source") : null;
        String dataSource = dataSourceNode != null && dataSourceNode.isTextual() ? dataSourceNode.asText() : defaultDataSource;
        Database database = databaseOf(dataSource);
        dataSources.put(database, dataSource);

        JsonNode procedureNode = entityDocument != null ? entityDocument.get("procedure") : null;
        boolean procedure = procedureNode != null && procedureNode.asBoolean();

        JsonNode titleNode = entityDocument != null ? entityDocument.get("title") : null;
        String title = titleNode != null && titleNode.isTextual() ? titleNode.asText() : anEntityName;
        JsonNode sqlNode = entityDocument != null ? entityDocument.get("sql") : null;
        String customSql = sqlNode != null && sqlNode.isTextual() ? sqlNode.asText() : null;
        JsonNode readonlyNode = entityDocument != null ? entityDocument.get("readonly") : null;
        boolean readonly = readonlyNode != null && readonlyNode.asBoolean();
        JsonNode publicNode = entityDocument != null ? entityDocument.get("public") : null;
        boolean publicAccess = publicNode != null && publicNode.asBoolean();
        JsonNode pageSizeNode = entityDocument != null ? entityDocument.get("pageSize") : null;
        int pageSize = pageSizeNode != null && pageSizeNode.isInt() ? pageSizeNode.asInt(DataProvider.NO_PAGING_PAGE_SIZE) : DataProvider.NO_PAGING_PAGE_SIZE;

        Statement querySyntax = compileEntities ? new SeptimaSqlParser().parse(new StringReader(anEntitySql)) : null;

        JsonNode commandNode = entityDocument != null ? entityDocument.get("command") : null;
        boolean command = commandNode != null && commandNode.isBoolean() ? commandNode.asBoolean() : querySyntax != null && !(querySyntax instanceof Select);

        Map<String, Parameter> params = querySyntax != null ? ExtractParameters.from(querySyntax) : new HashMap<>();
        // subQueryName, subQueryParameterName, parameterName
        Map<String, Map<String, String>> paramsBinds = new HashMap<>();
        JsonNode paramsNode = entityDocument != null ? entityDocument.get("parameters") : null;
        if (paramsNode != null && paramsNode.isObject()) {
            paramsNode.fields().forEachRemaining(parameterReader(params, paramsBinds));
        }

        if (querySyntax != null) {
            InlineEntities.to(querySyntax, this, paramsBinds, aStartOfReferences, aIllegalReferences);
        }
        String sqlWithSubQueries = querySyntax != null ? StatementDeParser.assemble(querySyntax) : null;
        Map<String, EntityField> fields = querySyntax != null ? columnsToApplicationFields(
                resolveColumnsBySyntax(database, querySyntax), database.getSqlDriver()
        ) : new HashMap<>();

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
                DataSource dataSource = Database.obtainDataSource(aDataSourceName);
                return new Database(
                        dataSource,
                        DataSources.getDataSourceSqlDriver(dataSource),
                        compileEntities ? Metadata.of(dataSource) : null,
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
