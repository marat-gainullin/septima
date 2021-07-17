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

public class EntitiesRaws extends EntitiesProcessor{

    private final String rowTemplate;
    private final String rowPropertyFieldTemplate;
    private final String rowPropertyFieldAccessorsTemplate;
    private final String forwardMappingTemplate;
    private final String narrowedForwardMappingTemplate;
    private final String reverseMappingTemplate;

    public static EntitiesRaws fromResources(SqlEntities anEntities, Path aDestination) throws IOException {
        return fromResources(anEntities, aDestination, StandardCharsets.UTF_8, System.lineSeparator());
    }

    public static EntitiesRaws fromResources(SqlEntities anEntities, Path aDestination, Charset aCharset, String aLf) throws IOException {
        return new EntitiesRaws(anEntities, aDestination,
                Utils.loadResource("/raw.template", aCharset, aLf),
                Utils.loadResource("/raw/property-field.template", aCharset, aLf),
                Utils.loadResource("/raw/property-field-accessors.template", aCharset, aLf),
                Utils.loadResource("/raw/forward-mapping.template", aCharset, aLf),
                Utils.loadResource("/raw/narrowed-forward-mapping.template", aCharset, aLf),
                Utils.loadResource("/raw/reverse-mapping.template", aCharset, aLf),
                aLf, aCharset);
    }

    private EntitiesRaws(SqlEntities anEntities, Path aDestination,
                         String aRowTemplate,
                         String aRowPropertyFieldTemplate,
                         String aRowPropertyFieldAccessorsTemplate,
                         String aForwardMappingTemplate,
                         String aNarrowedForwardMappingTemplate,
                         String aReverseMappingTemplate,
                         String aLf, Charset aCharset) {
        super(anEntities, aDestination, aLf, aCharset);
        rowTemplate = aRowTemplate;
        rowPropertyFieldTemplate = aRowPropertyFieldTemplate;
        rowPropertyFieldAccessorsTemplate = aRowPropertyFieldAccessorsTemplate;
        forwardMappingTemplate = aForwardMappingTemplate.replace(aLf, "");
        narrowedForwardMappingTemplate = aNarrowedForwardMappingTemplate.replace(aLf, "");
        reverseMappingTemplate = aReverseMappingTemplate.replace(aLf, "");
    }

    public Path considerJavaSource(Path sqlEntityFile) {
        Path entityRelativePath = entities.getEntitiesRoot().relativize(sqlEntityFile);
        String entityRelativePathName = entityRelativePath.toString().replace('\\', '/');
        String entityRef = entityRelativePathName.substring(0, entityRelativePathName.length() - 4);
        String entityBaseClassName = Utils.rawClass(entityRef.substring(entityRef.lastIndexOf('/') + 1));
        return destination.resolve(entityRelativePath.resolveSibling(entityBaseClassName + ".java"));
    }

    public Path toJavaSource(Path path) throws IOException {
        Path entityRelativePath = entities.getEntitiesRoot().relativize(path);
        Path entityRelativeDirPath = entityRelativePath.getParent();
        String entityRelativePathName = entityRelativePath.toString().replace('\\', '/');
        String entityRef = entityRelativePathName.substring(0, entityRelativePathName.length() - 4);
        SqlEntity entity = entities.loadEntity(entityRef);
        if (!entity.isCommand()) {
            String entityBaseClassName = Utils.rawClass(entityRef.substring(entityRef.lastIndexOf('/') + 1));
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
            StringBuilder forwardMappings = generateForwardMappings(entity);
            StringBuilder reverseMappings = generateReverseMappings(entity);
            String entityRow = Utils.replaceVariables(rowTemplate, Map.of(
                    "package", entityRelativeDirPath != null ?
                            ("package " + entityRelativeDirPath.toString().replace('\\', '/').replace('/', '.') + ";" + lf) :
                            "",
                    "dateImport", entity.getFields().values().stream().anyMatch(f -> GenericType.DATE == f.getType()) ? "import java.util.Date;" + lf : "",
                    "entityBaseClass", entityBaseClassName,
                    "propertiesFields", propertiesFields.toString(),
                    "propertiesAccessors", propertiesAccessors.toString(),
                    "forwardMappings", forwardMappings.toString(),
                    "reverseMappings", reverseMappings.toString()
            ), lf).toString();
            if (!entityClassFile.getParent().toFile().exists()) {
                entityClassFile.getParent().toFile().mkdirs();
            }
            Files.write(entityClassFile, entityRow.getBytes(charset));
            Logger.getLogger(EntitiesRaws.class.getName()).log(Level.INFO, "Sql entity definition '" + path + "' transformed and written to: " + entityClassFile);
            return entityClassFile;
        } else {
            throw new IllegalStateException("Can't transform a DML query '" + path + "' to an entity class");
        }
    }

    public int deepToJavaSources(Path source) throws IOException {
        return Files.walk(source)
                .filter(entityPath -> entityPath.toString().toLowerCase().endsWith(".sql"))
                .map(path -> {
                    try {
                        toJavaSource(path);
                        return 1;
                    } catch (UncheckedSQLException | UncheckedJSqlParserException | IOException ex) {
                        Logger.getLogger(EntitiesRaws.class.getName()).log(Level.SEVERE, "Sql entity definition '" + path + "' skipped due to an exception", ex);
                        return 0;
                    }
                })
                .reduce(Integer::sum)
                .orElse(0);
    }

    private StringBuilder generateForwardMappings(SqlEntity anEntity) {
        return anEntity.getFields().values().stream()
                .map(Utils.ModelField::new)
                .map(modelField -> Utils.replaceVariables(modelField.getGenericType() == GenericType.DOUBLE || modelField.getGenericType() == GenericType.LONG ? narrowedForwardMappingTemplate : forwardMappingTemplate, Map.of(
                        "propertyMutator", modelField.getPropertyMutator(),
                        "propertyType", modelField.getPropertyType(),
                        "fieldName", modelField.getFieldName(),
                        "genericType", (modelField.getGenericType() != null ? modelField.getGenericType() : GenericType.STRING).name()
                ), lf))
                .reduce((m1, m2) -> m1.append(lf).append(m2))
                .orElse(new StringBuilder());
    }

    private StringBuilder generateReverseMappings(SqlEntity anEntity) {
        return anEntity.getFields().values().stream()
                .map(Utils.ModelField::new)
                .map(modelField -> Utils.replaceVariables(reverseMappingTemplate, Map.of(
                        "propertyGetter", modelField.getPropertyGetter(),
                        "fieldName", modelField.getFieldName()
                ), lf))
                .reduce((m1, m2) -> m1.append(",").append(lf).append(m2))
                .orElse(new StringBuilder());
    }

}
