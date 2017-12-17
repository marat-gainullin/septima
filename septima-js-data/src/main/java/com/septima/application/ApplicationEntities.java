package com.septima.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.septima.Database;
import com.septima.EntitiesHost;
import com.septima.Parameter;
import com.septima.dataflow.DataProvider;
import com.septima.jdbc.DataSources;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.Field;
import com.septima.metadata.ForeignKey;
import com.septima.metadata.JdbcColumn;
import com.septima.queries.SqlEntity;
import com.septima.queries.InlineEntities;
import com.septima.sqldrivers.resolvers.TypesResolver;

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

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.SelectItems;
import net.sf.jsqlparser.FromItems;
import net.sf.jsqlparser.UncheckedJSQLParserException;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

/**
 * @author mg
 */
public class ApplicationEntities implements EntitiesHost {

    private static final String ENTITY_NAME_MISSING_MSG = "Entity name missing.";
    private static final String LOADING_QUERY_MSG = "Loading entity '%s'.";
    private static final String LOADED_QUERY_MSG = "Entity '%s' loaded.";

    private final Database database;
    private final Path applicationPath;
    private final Map<String, SqlEntity> entities = new ConcurrentHashMap<>();
    private final boolean aliasesToTableNames;

    public ApplicationEntities(Database aDatabase, Path aApplicationPath, boolean aAliasesToTableNames) {
        super();
        database = aDatabase;
        applicationPath = aApplicationPath;
        aliasesToTableNames = aAliasesToTableNames;
    }

    public SqlEntity loadEntity(String aEntityName) {
        Objects.requireNonNull(aEntityName, ENTITY_NAME_MISSING_MSG);
        return entities.computeIfAbsent(aEntityName, entityName -> {
            try {
                Logger.getLogger(ApplicationEntities.class.getName()).finer(String.format(LOADING_QUERY_MSG, aEntityName));
                String entityJsonFileName = aEntityName.replace(".", File.separator) + ".sql.json";
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
            SqlEntity query = loadEntity(anEntityName);
            if (query != null) {
                Map<String, Field> fields = query.getFields();
                Field resolved = fields.get(aFieldName);
                String resolvedTableName = resolved != null && resolved.getTableName() != null ? resolved.getTableName().toLowerCase() : null;
                return resolvedTableName != null &&
                        (query.getWritable().isEmpty() || query.getWritable().contains(resolvedTableName)) ? resolved : null;
            } else {
                // It seems, that anEntityName is a table name...
                Map<String, JdbcColumn> fields = database.getMetadata().getTable(anEntityName)
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

    private Map<String, Field> tableColumnsToApplicationFields(Table aTable) throws SQLException {
        Map<String, JdbcColumn> tableFields = database.getMetadata()
                .getTable(aTable.getWholeTableName())
                .orElse(Map.of());
        TypesResolver typesResolver = database.getSqlDriver().getTypesResolver();
        return tableFields.values().stream()
                .map((JdbcColumn column) -> new Field(
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
                                 * TODO: Add a test with dereference of tables from different schemas, including default schema
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
        String entitySqlFileName = aEntityName.replace(".", File.separator) + ".sql";
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
            // JsonNode defaultValueNode = parameterNode.get("value");
            // String defaultValue = defaultValueNode != null && defaultValueNode.isTextual() ? defaultValueNode.asText() : null;
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
                                        Map<String, String> subToOuterParams = paramsBinds.computeIfAbsent(subQueryName, sqn -> Map.of());
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
                                            ForeignKey.ForeignKeyRule.NOACTION, ForeignKey.ForeignKeyRule.NOACTION, false,
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

    private SqlEntity constructEntity(String anEntityName, String entitySql, File entityJsonFile) throws IOException, JSQLParserException, SQLException {
        Objects.requireNonNull(anEntityName, ENTITY_NAME_MISSING_MSG);
        JsonNode entityDocument = parseEntitySettings(entityJsonFile);
        JsonNode procedureNode = entityDocument != null ? entityDocument.get("procedure") : null;
        boolean procedure = procedureNode != null && procedureNode.asBoolean();

        Map<String, Parameter> params = new LinkedHashMap<>();
        // subQueryName, subQueryParameterName, parameterName
        Map<String, Map<String, String>> paramsBinds = new HashMap<>();
        JsonNode paramsNode = entityDocument != null ? entityDocument.get("parameters") : null;
        if (paramsNode != null && paramsNode.isObject()) {
            paramsNode.fields().forEachRemaining(parameterReader(params, paramsBinds));
        }

        String title;
        String customSql;
        boolean command;
        boolean readonly;
        boolean publicAccess;
        int pageSize;

        if (entitySql != null && !entitySql.isEmpty()) {
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Statement querySyntax = parserManager.parse(new StringReader(entitySql));

            InlineEntities.to(querySyntax, this, paramsBinds);
            String sqlWithSubQueries = StatementDeParser.assemble(querySyntax);
            Map<String, Field> fields = resolveFieldsBySyntax(querySyntax);

            Set<String> writable = new HashSet<>();
            Set<String> readRoles = new HashSet<>();
            Set<String> writeRoles = new HashSet<>();

            if (entityDocument != null) {
                JsonNode titleNode = entityDocument.get("title");
                title = titleNode != null && titleNode.isTextual() ? titleNode.asText() : anEntityName;
                JsonNode sqlNode = entityDocument.get("sql");
                customSql = sqlNode != null && sqlNode.isTextual() ? sqlNode.asText() : null;
                JsonNode commandNode = entityDocument.get("command");
                command = commandNode != null && commandNode.asBoolean();
                JsonNode readonlyNode = entityDocument.get("readonly");
                readonly = readonlyNode != null && readonlyNode.asBoolean();
                JsonNode publicNode = entityDocument.get("public");
                publicAccess = publicNode != null && publicNode.asBoolean();
                JsonNode pageSizeNode = entityDocument.get("pageSize");
                pageSize = pageSizeNode != null && pageSizeNode.isInt() ? pageSizeNode.asInt(DataProvider.NO_PAGING_PAGE_SIZE) : DataProvider.NO_PAGING_PAGE_SIZE;
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
            } else {
                title = anEntityName;
                customSql = null;
                readonly = false;
                command = false;
                procedure = false;
                publicAccess = false;
                pageSize = DataProvider.NO_PAGING_PAGE_SIZE;
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

    private Map<String, Field> resolveFieldsBySyntax(Statement parsedQuery) throws SQLException {
        if (parsedQuery instanceof Select) {
            Select select = (Select) parsedQuery;
            return resolveOutputFieldsFromSources(select.getSelectBody());
        } else {
            return Map.of();
        }
    }

    private Map<String, Field> resolveOutputFieldsFromSources(SelectBody aSelectBody) throws SQLException {
        Map<String, Field> fields = new LinkedHashMap<>();
        Map<String, FromItem> sources = FromItems.find(FromItems.ToCase.LOWER, aSelectBody);
        for (SelectItem selectItem : SelectItems.find(aSelectBody)) {
            if (selectItem instanceof AllColumns) {// *
                for (FromItem source : sources.values()) {
                    if (source instanceof Table) {
                        Map<String, Field> tableFields = tableColumnsToApplicationFields((Table) source);
                        fields.putAll(tableFields);
                    } else if (source instanceof SubSelect) {
                        Map<String, Field> subSelectFields = resolveOutputFieldsFromSources(((SubSelect) source).getSelectBody());
                        fields.putAll(subSelectFields);
                    }
                }
            } else if (selectItem instanceof AllTableColumns) {// t.*
                AllTableColumns cols = (AllTableColumns) selectItem;
                assert cols.getTable() != null : "<table>.* syntax must lead to .getTable() != null";
                FromItem source = sources.get(cols.getTable().getWholeTableName().toLowerCase());
                if (source instanceof Table) {
                    Map<String, Field> tableFields = tableColumnsToApplicationFields((Table) source);
                    fields.putAll(tableFields);
                } else if (source instanceof SubSelect) {
                    Map<String, Field> subSelectFields = resolveOutputFieldsFromSources(((SubSelect) source).getSelectBody());
                    fields.putAll(subSelectFields);
                }
            } else {
                assert selectItem instanceof SelectExpressionItem;
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                if (selectExpressionItem.getExpression() instanceof Column) {
                    Field field = resolveFieldByColumnExpression((Column) selectExpressionItem.getExpression(), selectExpressionItem.getAliasName(), sources);
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

    private Field resolveFieldBySource(Column column, String alias, FromItem source) throws SQLException {
        if (source instanceof Table) {
            Map<String, Field> tableFields = tableColumnsToApplicationFields((Table) source);
            Field resolved = tableFields.getOrDefault(column.getColumnName(), new Field(column.getColumnName()));
            return asAliasedField(resolved, column, alias, source);
        } else if (source instanceof SubSelect) {
            Map<String, Field> subFields = resolveOutputFieldsFromSources(((SubSelect) source).getSelectBody());
            Field resolved = subFields.getOrDefault(column.getColumnName(), new Field(column.getColumnName()));
            return asAliasedField(resolved, column, alias, source);
        } else {
            return new Field(column.getColumnName());
        }
    }

    private boolean isFieldInSource(Column column, FromItem source) throws SQLException {
        if (source instanceof Table) {
            Map<String, Field> tableFields = tableColumnsToApplicationFields((Table) source);
            return tableFields.containsKey(column.getColumnName());
        } else if (source instanceof SubSelect) {
            Map<String, Field> subFields = resolveOutputFieldsFromSources(((SubSelect) source).getSelectBody());
            return subFields.containsKey(column.getColumnName());
        } else {
            return false;
        }
    }

    private Field resolveFieldByColumnExpression(Column column, String alias, Map<String, FromItem> aSources) throws SQLException {
        if (column.getTable() != null &&
                column.getTable().getWholeTableName() != null &&
                !column.getTable().getWholeTableName().isEmpty()) {
            /*
             * Таблица поля, предоставляемая парсером никак не связана с
             * таблицей из списка from. Поэтому мы должны связать их
             * самостоятельно.
             */
            FromItem source = aSources.get(column.getTable().getWholeTableName().toLowerCase());
            return resolveFieldBySource(column, alias, source);
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
                            return !isFieldInSource(column, source);
                        } catch (SQLException ex) {
                            throw new UncheckedSQLException(ex);
                        }
                    })
                    .findFirst()
                    .map(source -> {
                        try {
                            return resolveFieldBySource(column, alias, source);
                        } catch (SQLException ex) {
                            throw new UncheckedSQLException(ex);
                        }
                    })
                    .orElse(new Field(alias != null && !alias.isEmpty() ? alias : column.getColumnName()));
        }
    }
}
