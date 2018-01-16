package com.septima.generator;

import com.septima.GenericType;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.EntityField;
import net.sf.jsqlparser.UncheckedJSqlParserException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EntitiesRows {

    private final String rowTemplate;
    private final String rowPropertyFieldAccessorsTemplate;
    private final String rowPropertyFieldTemplate;

    private final SqlEntities entities;
    private final Path destination;
    private final String lf;
    private final Charset charset;

    public static EntitiesRows fromResources(SqlEntities anEntities, Path aDestination) throws IOException {
        return fromResources(anEntities, aDestination, StandardCharsets.UTF_8, System.lineSeparator());
    }

    public static EntitiesRows fromResources(SqlEntities anEntities, Path aDestination, Charset aCharset, String aLf) throws IOException {
        return new EntitiesRows(anEntities, aDestination,
                Utils.loadResource("/row.template", aCharset, aLf),
                Utils.loadResource("/row/property-field.template", aCharset, aLf),
                Utils.loadResource("/row/property-field-accessors.template", aCharset, aLf),
                aLf, aCharset);
    }

    private EntitiesRows(SqlEntities anEntities, Path aDestination,
                         String aRowTemplate,
                         String aRowPropertyFieldTemplate,
                         String aRowPropertyFieldAccessorsTemplate,
                         String aLf, Charset aCharset) {
        entities = anEntities;
        destination = aDestination;
        rowTemplate = aRowTemplate;
        rowPropertyFieldTemplate = aRowPropertyFieldTemplate;
        rowPropertyFieldAccessorsTemplate = aRowPropertyFieldAccessorsTemplate;
        lf = aLf;
        charset = aCharset;
    }

    public Path considerJavaSource(Path sqlEntityFile) {
        Path entityRelativePath = entities.getEntitiesRoot().relativize(sqlEntityFile);
        String entityRelativePathName = entityRelativePath.toString().replace('\\', '/');
        String entityRef = entityRelativePathName.substring(0, entityRelativePathName.length() - 4);
        String entityBaseClassName = Utils.entityRowClass(entityRef.substring(entityRef.lastIndexOf('/') + 1));
        return destination.resolve(entityRelativePath.resolveSibling(entityBaseClassName + ".java"));
    }

    public Path toJavaSource(Path path) throws IOException {
        Path entityRelativePath = entities.getEntitiesRoot().relativize(path);
        Path entityRelativeDirPath = entityRelativePath.getParent();
        String entityRelativePathName = entityRelativePath.toString().replace('\\', '/');
        String entityRef = entityRelativePathName.substring(0, entityRelativePathName.length() - 4);
        SqlEntity entity = entities.loadEntity(entityRef);
        String entityBaseClassName = Utils.entityRowClass(entityRef.substring(entityRef.lastIndexOf('/') + 1));
        Path entityClassFile = destination.resolve(entityRelativePath.resolveSibling(entityBaseClassName + ".java"));

        StringBuilder propertiesFields = entity.getFields().values().stream()
                .sorted(Comparator.comparing(EntityField::getName))
                .map(Utils.ModelField::new)
                .map(f -> Utils.replaceVariables(rowPropertyFieldTemplate, Map.of(
                        "propertyType", f.getPropertyType(),
                        "property", f.getProperty()
                ), lf))
                .reduce(StringBuilder::append)
                .orElse(new StringBuilder());
        StringBuilder propertiesAccessors = entity.getFields().values().stream()
                .sorted(Comparator.comparing(EntityField::getName))
                .map(Utils.ModelField::new)
                .map(f -> Utils.replaceVariables(rowPropertyFieldAccessorsTemplate, Map.of(
                        "propertyType", f.getPropertyType(),
                        "propertyGetter", f.getPropertyGetter(),
                        "property", f.getProperty(),
                        "propertyMutator", f.getPropertyMutator(),
                        "fieldName", f.getFieldName()
                ), lf))
                .reduce((s1, s2) -> s1.append(lf).append(s2))
                .orElse(new StringBuilder());
        String entityRow = Utils.replaceVariables(rowTemplate, Map.of(
                "package", entityRelativeDirPath != null ?
                        ("package " + entityRelativeDirPath.toString().replace('\\', '/').replace('/', '.') + ";" + lf) :
                        "",
                "dateImport", entity.getFields().values().stream().anyMatch(f -> GenericType.DATE == f.getType()) ? lf + "import java.util.Date;" : "",
                "entityBaseClass", entityBaseClassName,
                "propertiesFields", propertiesFields.toString(),
                "propertiesAccessors", propertiesAccessors.toString()
        ), lf).toString();
        if (!entityClassFile.getParent().toFile().exists()) {
            entityClassFile.getParent().toFile().mkdirs();
        }
        Files.write(entityClassFile, entityRow.getBytes(charset));
        Logger.getLogger(EntitiesRows.class.getName()).log(Level.INFO, "Sql entity definition '" + path + "' transformed and written to: " + entityClassFile);
        return entityClassFile;
    }

    public int deepToJavaSources(Path source) throws IOException {
        return Files.walk(source)
                .filter(entityPath -> entityPath.toString().toLowerCase().endsWith(".sql"))
                .map(path -> {
                    try {
                        toJavaSource(path);
                        return 1;
                    } catch (UncheckedSQLException | UncheckedJSqlParserException | IOException ex) {
                        Logger.getLogger(EntitiesRows.class.getName()).log(Level.SEVERE, "Sql entity definition '" + path + "' skipped due to an exception", ex);
                        return 0;
                    }
                })
                .reduce(Integer::sum)
                .orElse(0);
    }

}
