package com.septima.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.Parameter;
import net.sf.jsqlparser.UncheckedJSqlParserException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class EntitiesSnapshots extends EntitiesProcessor {

    private static final ObjectWriter JSON_WRITER = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .writer();

    public EntitiesSnapshots(SqlEntities anEntities, Path aDestination) {
        this(anEntities, aDestination, System.lineSeparator(), StandardCharsets.UTF_8);
    }

    public EntitiesSnapshots(SqlEntities anEntities, Path aDestination,
                             String aLf, Charset aCharset) {
        super(anEntities, aDestination, aLf, aCharset);
    }

    public Path considerSnapshotJson(Path sqlEntityFile) {
        return destination
                .resolve(entities.getEntitiesRoot()
                        .relativize(sqlEntityFile.resolveSibling(
                                sqlEntityFile.getFileName() + ".json")
                        )
                );
    }

    /**
     * Returns a {@link Map.Entry} with specified key and value.
     * Unlike {@link Map#entry(Object, Object)} retains {@code null} values to preserve datum structure.
     *
     * @param aKey   A key.
     * @param aValue A value.
     * @param <K>    Map's key type.
     * @param <V>    Map's value type.
     * @return A {@link Map.Entry} with specified key and value.
     */
    public static <K, V> Map.Entry<K, V> entry(K aKey, V aValue) {
        return new AbstractMap.SimpleEntry<>(aKey, aValue);
    }

    /**
     * Returns map with specified entries.
     * Unlike {@link Map#ofEntries(Map.Entry[])} retains {@code null} values to preserve datum structure.
     *
     * @param aEntries An array of entries.
     * @param <K>      Map's key type.
     * @param <V>      Map's value type.
     * @return A map with specified entries.
     * Unlike {@link Map#ofEntries(Map.Entry[])} retains {@code null} values to preserve datum structure.
     */
    public static <K, V> Map<K, V> map(Map.Entry<K, V>... aEntries) {
        Map<K, V> map = new HashMap<>();
        for (Map.Entry<K, V> e : aEntries) {
            Objects.requireNonNull(e.getKey());
            map.put(e.getKey(), e.getValue());
        }
        return Collections.unmodifiableMap(map);
    }

    public Path toSnapshotJson(Path sqlEntityFile) throws IOException {
        Path entityRelativePath = entities.getEntitiesRoot().relativize(sqlEntityFile);
        String entityRelativePathName = entityRelativePath.toString().replace('\\', '/');
        String entityRef = entityRelativePathName.substring(0, entityRelativePathName.length() - 4);
        SqlEntity entity = entities.loadEntity(entityRef);

        Path jsonFile = considerSnapshotJson(sqlEntityFile);
        if (!jsonFile.getParent().toFile().exists()) {
            jsonFile.getParent().toFile().mkdirs();
        }
        JSON_WRITER.writeValue(jsonFile.toFile(), map(
                entry("title", entity.getTitle()),
                entry("source", entities.dataSourceOf(entity.getDatabase())),
                entry("public", entity.isPublicAccess()),
                entry("sql", entity.getCustomSqlText() != null && !entity.getCustomSqlText().isBlank() ? entity.getCustomSqlText() : entity.getSqlText()),
                entry("procedure", entity.isProcedure()),
                entry("command", entity.isCommand()),
                entry("readonly", entity.isReadonly()),
                entry("parameters", entity.getParameters().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> map(
                        entry("type", e.getValue().getType() != null ? e.getValue().getType().getText() : null),
                        entry("subType", e.getValue().getSubType()),
                        entry("description", e.getValue().getDescription()),
                        entry("value", e.getValue().getValue()),
                        entry("out", e.getValue().getMode() == Parameter.Mode.InOut || e.getValue().getMode() == Parameter.Mode.Out)
                )))),
                entry("fields", entity.getFields().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> map(
                        entry("nullable", e.getValue().isNullable()),
                        entry("type", e.getValue().getType() != null ? e.getValue().getType().getText() : null),
                        entry("subType", e.getValue().getSubType()),
                        entry("description", e.getValue().getDescription()),
                        entry("tableName", e.getValue().getTableName()),
                        entry("originalName", e.getValue().getOriginalName()),
                        entry("key", e.getValue().isPk()),
                        entry("reference", e.getValue().getFk() != null ? Map.of(
                                "entity", e.getValue().getFk().getReferee().getTable(), "key", e.getValue().getFk().getReferee().getColumn()
                        ) : null)
                )))),
                entry("writable", entity.getWritable()),
                entry("roles", Map.of(
                        "read", entity.getReadRoles(), "write", entity.getWriteRoles()
                )),
                entry("pageSize", entity.getPageSize())
        ));
        return jsonFile;
    }

    public int deepToSnapshotJsons(Path source) throws IOException {
        return Files.walk(source)
                .filter(entityPath -> entityPath.toString().toLowerCase().endsWith(".sql"))
                .map(path -> {
                    try {
                        toSnapshotJson(path);
                        return 1;
                    } catch (UncheckedSQLException | UncheckedJSqlParserException | IOException ex) {
                        Logger.getLogger(EntitiesSnapshots.class.getName()).log(Level.SEVERE, "Sql entity definition '" + path + "' skipped due to an exception", ex);
                        return 0;
                    }
                })
                .reduce(Integer::sum)
                .orElse(0);
    }
}
