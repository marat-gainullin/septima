package com.septima.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.septima.Database;
import com.septima.Entities;
import com.septima.Parameter;
import com.septima.dataflow.DataProvider;
import com.septima.jdbc.DataSources;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.Field;
import com.septima.metadata.ForeignKey;
import com.septima.metadata.JdbcColumn;
import com.septima.queries.InlineEntities;
import com.septima.queries.SqlEntity;
import com.septima.sqldrivers.resolvers.TypesResolver;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.JSqlParser;
import net.sf.jsqlparser.SeptimaSqlParser;
import net.sf.jsqlparser.UncheckedJSQLParserException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.syntax.FromItems;
import net.sf.jsqlparser.syntax.SelectItems;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author mg
 */
public class ApplicationEntities implements Entities {

    private static final String ENTITY_NAME_MISSING_MSG = "Entity name missing.";
    private static final String LOADING_QUERY_MSG = "Loading entity '%s'.";
    private static final String LOADED_QUERY_MSG = "Entity '%s' loaded.";

    private final Path applicationPath;
    private final String defaultDataSource;
    private final Map<String, SqlEntity> entities = new ConcurrentHashMap<>();
    private final boolean aliasesToTableNames;

    public ApplicationEntities(Path aApplicationPath, String aDefaultDataSource) {
        this(aApplicationPath, aDefaultDataSource, false);
    }

    public ApplicationEntities(Path aApplicationPath, String aDefaultDataSource, boolean aAliasesToTableNames) {
        super();
        applicationPath = aApplicationPath;
        defaultDataSource = aDefaultDataSource;
        aliasesToTableNames = aAliasesToTableNames;
    }

    public Path getApplicationPath() {
        return applicationPath;
    }

    public String getDefaultDataSource() {
        return defaultDataSource;
    }

    public SqlEntity loadEntity(String aEntityName) {
        Objects.requireNonNull(aEntityName, ENTITY_NAME_MISSING_MSG);
        return entities.computeIfAbsent(aEntityName, entityName -> {
            try {
                Logger.getLogger(ApplicationEntities.class.getName()).finer(String.format(LOADING_QUERY_MSG, aEntityName));
                String entityJsonFileName = aEntityName + ".sql.json";
                File entityJsonFile = applicationPath.resolve(entityJsonFileName).toFile();
                SqlEntity entity = constructEntity(aEntityName, readEntitySql(aEntityName), entityJsonFile);
                Logger.getLogger(ApplicationEntities.class.getName()).finer(String.format(LOADED_QUERY_MSG, aEntityName));
                return entity;
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            } catch (JSQLParserException ex) {
                throw new UncheckedJSQLParserException(ex);
            } catch (SQLException ex) {
                throw new UncheckedSQLException(ex);
            }
        });
    }

    @Override
    public Parameter resolveParameter(String aEntityName, String aParamName) {
        if (aEntityName != null) {
            SqlEntity entity = loadEntity(aEntityName);
            if (entity != null) {
                return entity.getParameters().get(aParamName);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public Field resolveField(String anEntityName, String aFieldName) throws SQLException {
        if (anEntityName != null) {
            SqlEntity entity = loadEntity(anEntityName);
            if (entity != null) {
                Map<String, Field> fields = entity.getFields();
                Field resolved = fields.get(aFieldName);
                String resolvedTableName = resolved != null && resolved.getTableName() != null ? resolved.getTableName().toLowerCase() : null;
                return resolvedTableName != null &&
                        (entity.getWritable().isEmpty() || entity.getWritable().contains(resolvedTableName)) ? resolved : null;
            } else {
                // It seems, that anEntityName is a table name...
                Map<String, JdbcColumn> fields = Database.of(defaultDataSource).getMetadata().getTableColumns(anEntityName)
                        .orElseGet(() -> {
                            Logger.getLogger(DataSources.class.getName()).log(Level.WARNING, "Can't find fields for entity '{0}'", anEntityName);
                            return Map.of();
                        });
                return fields.get(aFieldName);
            }
        } else {
            return null;
        }
    }

    private Map<String, Field> tableColumnsToApplicationFields(Database database, Table aTable) throws SQLException {
        Map<String, JdbcColumn> tableFields = database.getMetadata()
                .getTableColumns(aTable.getWholeTableName())
                .orElse(Map.of());
        TypesResolver typesResolver = database.getSqlDriver().getTypesResolver();
        return tableFields.values().stream()
                .map(column -> new Field(
                                column.getName(),
                                column.getDescription(),
                                column.getOriginalName(),
                                /*
                                 * Всегда заменять имя оригинальной таблицы нельзя, особенно если
                                 * это поле ключевое т.к. при установлении связи по этим
                                 * полям будут проблемы. ORM-у придется "разматывать"
                                 * источник поля до таблицы чтобы восстановиит связи по
                                 * ключам. Здесь это делается исключительно ради очень
                                 * специального использования фабрики в дизайнере запросов.
                                 * TODO: Add a test with dereference indices tables from different schemas, including default schema
                                 */
                                aliasesToTableNames &&
                                        aTable.getAlias() != null && aTable.getAlias().getName() != null && !aTable.getAlias().getName().isEmpty() ?
                                        aTable.getAlias().getName() :
                                        column.getTableName(),
                                typesResolver.toApplicationType(column.getJdbcType(), column.getType()),
                                column.isNullable(),
                                column.isPk(),
                                column.getFk()
                        )
                )
                .collect(Collectors.toMap(Field::getName, Function.identity()));
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
            String type = typeNode != null && typeNode.isTextual() ? typeNode.asText(parameter.getType()) : parameter.getType();
            JsonNode descriptionNode = parameterNode.get("description");
            String description = descriptionNode != null && descriptionNode.isTextual() ? descriptionNode.asText(parameter.getDescription()) : parameter.getDescription();
            JsonNode valueNode = parameterNode.get("value");
            String value = valueNode != null && valueNode.isTextual() ? valueNode.asText() : null;
            JsonNode outNode = parameterNode.get("out");
            boolean out = outNode != null && outNode.isBoolean() && outNode.asBoolean();
            params.put(parameterName, new Parameter(
                    parameterName,
                    description,
                    type,
                    value,
                    out ? Parameter.Mode.InOut : Parameter.Mode.In
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

    private static Consumer<Map.Entry<String, JsonNode>> fieldReader(Map<String, Field> fields, String entityName) {
        return (fieldEntry) -> {
            String fieldName = fieldEntry.getKey();
            Field field = fields.getOrDefault(fieldName, new Field(fieldName));
            JsonNode fieldNode = fieldEntry.getValue();
            JsonNode typeNode = fieldNode.get("type");
            String type = typeNode != null && typeNode.isTextual() ? typeNode.asText(field.getType()) : field.getType();
            JsonNode descriptionNode = fieldNode.get("description");
            String description = descriptionNode != null && descriptionNode.isTextual() ? descriptionNode.asText(field.getDescription()) : field.getDescription();
            JsonNode nullableNode = fieldNode.get("nullable");
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
                                            null, entityName, fieldName, null,
                                            ForeignKey.ForeignKeyRule.NO_ACTION, ForeignKey.ForeignKeyRule.NO_ACTION, false,
                                            null, referencedEntity, referencedKey, null)
                                    : field.getFk()
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

    private SqlEntity constructEntity(String anEntityName, String anEntitySql, File anEntityJsonFile) throws IOException, JSQLParserException, SQLException {
        Objects.requireNonNull(anEntityName, ENTITY_NAME_MISSING_MSG);
        Objects.requireNonNull(anEntitySql, "anEntitySql is required argument");
        Objects.requireNonNull(anEntityJsonFile, "anEntityJsonFile is required argument");

        if (!anEntitySql.isEmpty()) {
            JsonNode entityDocument = parseEntitySettings(anEntityJsonFile);

            JsonNode dataSourceNode = entityDocument != null ? entityDocument.get("source") : null;
            String dataSource = dataSourceNode != null && dataSourceNode.isTextual() ? dataSourceNode.asText() : defaultDataSource;
            Database database = Database.of(dataSource);
            JsonNode procedureNode = entityDocument != null ? entityDocument.get("procedure") : null;
            boolean procedure = procedureNode != null && procedureNode.asBoolean();

            Map<String, Parameter> params = new LinkedHashMap<>();
            // subQueryName, subQueryParameterName, parameterName
            Map<String, Map<String, String>> paramsBinds = new HashMap<>();
            JsonNode paramsNode = entityDocument != null ? entityDocument.get("parameters") : null;
            if (paramsNode != null && paramsNode.isObject()) {
                paramsNode.fields().forEachRemaining(parameterReader(params, paramsBinds));
            }

            JsonNode titleNode = entityDocument != null ? entityDocument.get("title") : null;
            String title = titleNode != null && titleNode.isTextual() ? titleNode.asText() : anEntityName;
            JsonNode sqlNode = entityDocument != null ? entityDocument.get("sql") : null;
            String customSql = sqlNode != null && sqlNode.isTextual() ? sqlNode.asText() : null;
            JsonNode commandNode = entityDocument != null ? entityDocument.get("command") : null;
            boolean command = commandNode != null && commandNode.asBoolean();
            JsonNode readonlyNode = entityDocument != null ? entityDocument.get("readonly") : null;
            boolean readonly = readonlyNode != null && readonlyNode.asBoolean();
            JsonNode publicNode = entityDocument != null ? entityDocument.get("public") : null;
            boolean publicAccess = publicNode != null && publicNode.asBoolean();
            JsonNode pageSizeNode = entityDocument != null ? entityDocument.get("pageSize") : null;
            int pageSize = pageSizeNode != null && pageSizeNode.isInt() ? pageSizeNode.asInt(DataProvider.NO_PAGING_PAGE_SIZE) : DataProvider.NO_PAGING_PAGE_SIZE;

            JSqlParser sqlParser = new SeptimaSqlParser();
            Statement querySyntax = sqlParser.parse(new StringReader(anEntitySql));

            InlineEntities.to(querySyntax, this, paramsBinds, anEntityJsonFile.getParentFile().toPath());
            String sqlWithSubQueries = StatementDeParser.assemble(querySyntax);
            Map<String, Field> fields = resolveFieldsBySyntax(database, querySyntax);

            Set<String> writable = new HashSet<>();
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
        } else {
            throw new IllegalStateException(anEntityName + " has an empty sql text.");
        }
    }

    private Map<String, Field> resolveFieldsBySyntax(Database database, Statement parsedQuery) throws SQLException {
        if (parsedQuery instanceof Select) {
            Select select = (Select) parsedQuery;
            return resolveOutputFieldsFromSources(database, select.getSelectBody());
        } else {
            return Map.of();
        }
    }

    private Map<String, Field> resolveOutputFieldsFromSources(Database database, SelectBody aSelectBody) throws SQLException {
        Map<String, Field> fields = new LinkedHashMap<>();
        Map<String, FromItem> sources = FromItems.find(FromItems.ToCase.LOWER, aSelectBody);
        for (SelectItem selectItem : SelectItems.find(aSelectBody)) {
            if (selectItem instanceof AllColumns) {// *
                for (FromItem source : sources.values()) {
                    if (source instanceof Table) {
                        Map<String, Field> tableFields = tableColumnsToApplicationFields(database, (Table) source);
                        fields.putAll(tableFields);
                    } else if (source instanceof SubSelect) {
                        Map<String, Field> subSelectFields = resolveOutputFieldsFromSources(database, ((SubSelect) source).getSelectBody());
                        fields.putAll(subSelectFields);
                    }
                }
            } else if (selectItem instanceof AllTableColumns) {// t.*
                AllTableColumns cols = (AllTableColumns) selectItem;
                assert cols.getTable() != null : "<table>.* syntax must lead to .getTable() != null";
                FromItem source = sources.get(cols.getTable().getWholeTableName().toLowerCase());
                if (source instanceof Table) {
                    Map<String, Field> tableFields = tableColumnsToApplicationFields(database, (Table) source);
                    fields.putAll(tableFields);
                } else if (source instanceof SubSelect) {
                    Map<String, Field> subSelectFields = resolveOutputFieldsFromSources(database, ((SubSelect) source).getSelectBody());
                    fields.putAll(subSelectFields);
                }
            } else {
                assert selectItem instanceof SelectExpressionItem;
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                if (selectExpressionItem.getExpression() instanceof Column) {
                    Field field = resolveFieldByColumnExpression(database, (Column) selectExpressionItem.getExpression(), selectExpressionItem.getAliasName(), sources);
                    fields.put(field.getName(), field);
                } else {
                    /*
                     * free expression like a ...,'text' as txt,...
                     */
                    if (!selectExpressionItem.getAliasName().isEmpty()) {
                        Field field = new Field(selectExpressionItem.getAliasName());
                        fields.put(field.getName(), field);
                    } //else {
                    // Unnamed expression fields will be replaced by fact fields during data receiving from a database
                    //}
                }
            }
        }
        return fields;
    }

    private Field asAliasedField(Field resolved, Column column, String alias, FromItem source) {
        return new Field(
                alias != null && !alias.isEmpty() ? alias : column.getColumnName(),
                resolved.getDescription(),
                resolved.getOriginalName(),
                aliasesToTableNames && source.getAlias() != null && !source.getAlias().getName().isEmpty() ?
                        source.getAlias().getName() :
                        resolved.getTableName(),
                resolved.getType(),
                resolved.isNullable(),
                resolved.isPk(),
                resolved.getFk()
        );
    }

    private Field resolveFieldBySource(Database database, Column column, String alias, FromItem source) throws SQLException {
        if (source instanceof Table) {
            Map<String, Field> tableFields = tableColumnsToApplicationFields(database, (Table) source);
            Field resolved = tableFields.getOrDefault(column.getColumnName(), new Field(column.getColumnName()));
            return asAliasedField(resolved, column, alias, source);
        } else if (source instanceof SubSelect) {
            Map<String, Field> subFields = resolveOutputFieldsFromSources(database, ((SubSelect) source).getSelectBody());
            Field resolved = subFields.getOrDefault(column.getColumnName(), new Field(column.getColumnName()));
            return asAliasedField(resolved, column, alias, source);
        } else {
            return new Field(column.getColumnName());
        }
    }

    private boolean isFieldInSource(Database database, Column column, FromItem source) throws SQLException {
        if (source instanceof Table) {
            Map<String, Field> tableFields = tableColumnsToApplicationFields(database, (Table) source);
            return tableFields.containsKey(column.getColumnName());
        } else if (source instanceof SubSelect) {
            Map<String, Field> subFields = resolveOutputFieldsFromSources(database, ((SubSelect) source).getSelectBody());
            return subFields.containsKey(column.getColumnName());
        } else {
            return false;
        }
    }

    private Field resolveFieldByColumnExpression(Database database, Column column, String alias, Map<String, FromItem> aSources) throws SQLException {
        if (column.getTable() != null &&
                column.getTable().getWholeTableName() != null &&
                !column.getTable().getWholeTableName().isEmpty()) {
            /*
             * Таблица поля, предоставляемая парсером никак не связана с
             * таблицей из списка from. Поэтому мы должны связать их
             * самостоятельно.
             */
            FromItem source = aSources.get(column.getTable().getWholeTableName().toLowerCase());
            return resolveFieldBySource(database, column, alias, source);
        } else {
            /*
             * Часто бывает, что таблица из которого берется поле не указана.
             * Поэтому парсер не предоставляет таблицу.
             * В этом случае поищем первую таблицу, содержащую поле с таким именем.
             * Замечание: Таблица и подзапрос для парсера это одно и то же.
             * Замечание: Таблица или подзапрос может быть указан как имя настоящей таблицы или как алиас таблицы или подзапроса.
             */
            return aSources.values().stream()
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
                            return resolveFieldBySource(database, column, alias, source);
                        } catch (SQLException ex) {
                            throw new UncheckedSQLException(ex);
                        }
                    })
                    .orElse(new Field(alias != null && !alias.isEmpty() ? alias : column.getColumnName()));
        }
    }
}
