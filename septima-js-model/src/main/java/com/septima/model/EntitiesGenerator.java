package com.septima.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.septima.GenericType;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.EntityField;
import com.septima.metadata.Field;
import net.sf.jsqlparser.UncheckedJSqlParserException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class EntitiesGenerator {

    private final String modelTemplate;
    private final String modelEntityGetterTemplate;
    private final String modelEntityTemplate;
    private final String forwardMappingTemplate;
    private final String reverseMappingTemplate;
    private final String groupDeclarationTemplate;
    private final String groupFulfillTemplate;
    private final String requiredNavigationPropertyTemplate;
    private final String nullableNavigationPropertyTemplate;

    private final SqlEntities entities;
    private final Path source;
    private final Path destination;
    private final String indent;
    private final String lf;
    private final Charset charset;

    public static EntitiesGenerator fromResources(SqlEntities anEntities, Path aSource, Path aDestination) throws IOException, URISyntaxException {
        return fromResources(anEntities, aSource, aDestination, StandardCharsets.UTF_8);
    }

    public static EntitiesGenerator fromResources(SqlEntities anEntities, Path aSource, Path aDestination, Charset aCharset) throws IOException, URISyntaxException {
        return new EntitiesGenerator(anEntities, aSource, aDestination,
                new String(Files.readAllBytes(Paths.get(EntitiesGenerator.class.getResource("model.template").toURI())), aCharset),
                new String(Files.readAllBytes(Paths.get(EntitiesGenerator.class.getResource("model-entity.template").toURI())), aCharset),
                new String(Files.readAllBytes(Paths.get(EntitiesGenerator.class.getResource("model-entity-getter.template").toURI())), aCharset),
                new String(Files.readAllBytes(Paths.get(EntitiesGenerator.class.getResource("model-entity/forward-mapping.template").toURI())), aCharset),
                new String(Files.readAllBytes(Paths.get(EntitiesGenerator.class.getResource("model-entity/reverse-mapping.template").toURI())), aCharset),
                new String(Files.readAllBytes(Paths.get(EntitiesGenerator.class.getResource("model-entity/group-declaration.template").toURI())), aCharset),
                new String(Files.readAllBytes(Paths.get(EntitiesGenerator.class.getResource("model-entity/group-fulfill.template").toURI())), aCharset),
                new String(Files.readAllBytes(Paths.get(EntitiesGenerator.class.getResource("model-entity/required-navigation-property.template").toURI())), aCharset),
                new String(Files.readAllBytes(Paths.get(EntitiesGenerator.class.getResource("model-entity/nullable-navigation-property.template").toURI())), aCharset),
                "    ", System.getProperty("line.separator"), aCharset);
    }

    public EntitiesGenerator(SqlEntities anEntities, Path aSource, Path aDestination,
                             String aModelTemplate,
                             String aModelEntityTemplate,
                             String aModelEntityGetterTemplate,
                             String aForwardMappingTemplate,
                             String aReverseMappingTemplate,
                             String aGroupDeclarationTemplate,
                             String aGroupFulfillTemplate,
                             String aRequiredNavigationPropertyTemplate,
                             String aNullableNavigationPropertyTemplate,
                             String aIndent, String aLf, Charset aCharset) {
        entities = anEntities;
        source = aSource;
        destination = aDestination;
        modelTemplate = aModelTemplate;
        modelEntityTemplate = aModelEntityTemplate;
        modelEntityGetterTemplate = aModelEntityGetterTemplate;
        forwardMappingTemplate = aForwardMappingTemplate;
        reverseMappingTemplate = aReverseMappingTemplate;
        groupDeclarationTemplate = aGroupDeclarationTemplate;
        groupFulfillTemplate = aGroupFulfillTemplate;
        requiredNavigationPropertyTemplate = aRequiredNavigationPropertyTemplate;
        nullableNavigationPropertyTemplate = aNullableNavigationPropertyTemplate;
        indent = aIndent;
        lf = aLf;
        charset = aCharset;
    }

    private class ModelField {
        private final String propertyType;
        private final String property;
        private final String propertyGetter;
        private final String propertyMutator;
        private final String mutatorArg;
        private final String fieldName;

        private ModelField(EntityField field) {
            propertyType = javaType(field);
            String accessor = accessor(field);
            property = accessor.substring(0, 1).toLowerCase() + accessor.substring(1);
            propertyGetter = "get" + accessor;
            propertyMutator = "set" + accessor;
            mutatorArg = mutatorArg(field);
            fieldName = field.getName();
        }
    }

    public int generateRows() throws IOException {
        return Files.walk(source)
                .filter(entityPath -> entityPath.toString().toLowerCase().endsWith(".sql"))
                .map(path -> {
                    Path entityRelativePath = entities.getApplicationPath().relativize(path);
                    Path entityRelativeDirPath = entityRelativePath.getParent();
                    String entityRelativePathName = entityRelativePath.toString().replace('\\', '/');
                    String entityRef = entityRelativePathName.substring(0, entityRelativePathName.length() - 4);
                    try {
                        SqlEntity entity = entities.loadEntity(entityRef);
                        StringBuilder propertiesFields = entity.getFields().values().stream()
                                .sorted(Comparator.comparing(EntityField::getName))
                                .map(ModelField::new)
                                .map(f ->
                                        new StringBuilder(indent).append("private").append(" ").append(f.propertyType).append(" ").append(f.property).append(";").append(lf))
                                .reduce(StringBuilder::append)
                                .orElse(new StringBuilder());
                        StringBuilder propertiesGettersMutators = entity.getFields().values().stream()
                                .sorted(Comparator.comparing(EntityField::getName))
                                .map(ModelField::new)
                                .map(f -> new StringBuilder(""
                                                + indent + "public " + f.propertyType + " " + f.propertyGetter + "() {" + lf
                                                + indent + indent + "return " + f.property + ";" + lf
                                                + indent + "}" + lf
                                                + lf
                                                + indent + "public void " + f.propertyMutator + "(" + f.propertyType + " " + f.mutatorArg + ") {" + lf
                                                + indent + indent + f.propertyType + " old = " + f.property + ";" + lf
                                                + indent + indent + f.property + " = " + f.mutatorArg + ";" + lf
                                                + indent + indent + "changeSupport.firePropertyChange(\"" + f.fieldName + "\", old, " + f.property + ");" + lf
                                                + indent + "}" + lf
                                                + lf
                                        )
                                )
                                .reduce(StringBuilder::append)
                                .orElse(new StringBuilder());
                        String entityClassName = entityClass(entity.getClassName() != null && !entity.getClassName().isEmpty() ?
                                entity.getClassName() :
                                entity.getName().substring(entity.getName().lastIndexOf('/') + 1)
                        );
                        String entityRowClassName = entityRowClass(entityClassName);
                        String entityRow = "package " + entityRelativeDirPath.toString().replace('\\', '/').replace('/', '.') + ";" + lf
                                + lf
                                + "import com.septima.model.Observable;" + lf
                                + (entity.getFields().values().stream().anyMatch(f -> GenericType.DATE == f.getType()) ? "import java.util.Date;" + lf : "")
                                + lf
                                + "public class " + entityRowClassName + " extends Observable {" + lf
                                + lf
                                + propertiesFields
                                + lf
                                + propertiesGettersMutators
                                + "}"
                                + lf;
                        Path entityClassFile = destination.resolve(entityRelativeDirPath.resolve(entityRowClassName + ".java"));
                        if (!entityClassFile.getParent().toFile().exists()) {
                            entityClassFile.getParent().toFile().mkdirs();
                        }
                        Files.write(entityClassFile, entityRow.getBytes(charset));
                        return 1;
                    } catch (UncheckedSQLException | UncheckedJSqlParserException | IOException ex) {
                        Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.SEVERE, "Entity '" + entityRef + "' skipped due to an exception", ex);
                        return 0;
                    }
                })
                .reduce(Integer::sum)
                .orElse(0);
    }

    public int generateModels() throws IOException {
        return Files.walk(source)
                .filter(path -> path.toString().toLowerCase().endsWith(".model.json"))
                .map(modelPath -> {
                    try {
                        Path modelRelativePath = entities.getApplicationPath().relativize(modelPath).normalize();
                        Path modelRelativeDirPath = modelRelativePath.getParent();
                        String modelClassName = entityClass(modelRelativePath.getFileName().toString().substring(0, 11));
                        ObjectMapper jsonMapper = new ObjectMapper();
                        JsonNode modelDocument = jsonMapper.readTree(modelPath.toFile());
                        if (modelDocument != null && modelDocument.isObject()) {
                            Map<String, ModelEntity> modelEntities = readModelEntities(modelDocument, modelPath);
                            String modelBody = generateModelBody(modelEntities);
                            modelBody
                                    .replaceAll("\\$\\{modelPackage\\}", modelRelativeDirPath.toString().replace('\\', '/').replace('/', '.'))
                                    .replaceAll("\\$\\{modelClass\\}", modelClassName);
                            Path modelClassFile = destination.resolve(modelRelativeDirPath.resolve(modelClassName + ".java"));
                            if (!modelClassFile.getParent().toFile().exists()) {
                                modelClassFile.getParent().toFile().mkdirs();
                            }
                            Files.write(modelClassFile, modelBody.getBytes(charset));
                            return 1;
                        } else {
                            Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.INFO, "Bad '*.model.json' format detected in '" + modelPath + "'. 'sqlEntities' is required object in model definition");
                            return 0;
                        }
                    } catch (IOException ex) {
                        return 0;
                    }
                })
                .reduce(Integer::sum)
                .orElse(0);
    }

    class Reference {
        private final String field;
        private final String target;
        private final String scalar;
        private final String scalarGetter;
        private final String scalarMutator;
        private final String collectionGetter;
        private final String collection;
        private final String referenceGetter;
        private final String referenceMutator;

        private Reference(String aField, String aTarget, String aScalar, String aCollection) {
            field = aField;
            target = aTarget;
            scalar = aScalar;
            scalarGetter = "get" + capitalize(scalar);
            scalarMutator = "set" + capitalize(scalar);
            collection = aCollection;
            collectionGetter = "get" + capitalize(collection);
            referenceGetter = "get" + capitalize(field);
            referenceMutator = "set" + capitalize(field);
        }
    }

    class ModelEntity {

        private final String modelName;
        private final Collection<Reference> references;
        private final SqlEntity entity;
        private final EntityField keyField;
        private final String keyType;
        private final String keyGetter;
        private final String className;
        private final String baseClassName;
        private final String baseClassPackage;

        private ModelEntity(
                String aModelName,
                SqlEntity aEntity,
                EntityField aKeyField,
                Collection<Reference> aReferences
        ) {
            modelName = aModelName;
            references = aReferences;
            entity = aEntity;
            keyField = aKeyField;
            keyType = javaType(aKeyField, true);
            keyGetter = "get" + accessor(aKeyField);
            className = entityClass(entity.getClassName() != null && !entity.getClassName().isEmpty() ?
                    entity.getClassName() :
                    entity.getName().substring(entity.getName().lastIndexOf('/') + 1)
            );
            baseClassName = entityRowClass(className);
            baseClassPackage = entity.getName().substring(0, entity.getName().lastIndexOf('/')).replace('/', '.');
        }
    }

    private String generateGroupsDeclarations(ModelEntity aEntity, Map<String, ModelEntity> modelEntities) {
        return aEntity.references.stream()
                .filter(reference -> modelEntities.containsKey(reference.target))
                .map(reference -> new StringBuilder(
                        groupDeclarationTemplate
                                .replaceAll("\\$\\{entityKeyType\\}", aEntity.keyType)
                                .replaceAll("\\$\\{entityClass\\}", aEntity.className)
                                .replaceAll("\\$\\{entityQuery\\}", aEntity.modelName)
                                .replaceAll("\\$\\{scalarClass\\}", modelEntities.get(reference.target).className)
                ))
                .reduce(StringBuilder::append)
                .toString();
    }

    private String generateGroupsFulfills(ModelEntity aEntity, Map<String, ModelEntity> modelEntities) {
        return aEntity.references.stream()
                .filter(reference -> modelEntities.containsKey(reference.target))
                .map(reference -> new StringBuilder(
                        groupFulfillTemplate
                                .replaceAll("\\$\\{entityQuery\\}", aEntity.modelName)
                                .replaceAll("\\$\\{scalarClass\\}", modelEntities.get(reference.target).className)
                                .replaceAll("\\$\\{referenceGetter\\}", reference.referenceGetter)
                ))
                .reduce(StringBuilder::append)
                .toString();
    }

    private String generateForwardMappings(ModelEntity aEntity) {
        return aEntity.entity.getFields().values().stream()
                .map(ModelField::new)
                .map(modelField -> new StringBuilder(
                        forwardMappingTemplate
                                .replaceAll("\\$\\{propertyMutator\\}", modelField.propertyMutator)
                                .replaceAll("\\$\\{propertyType\\}", modelField.propertyType)
                                .replaceAll("\\$\\{fieldName\\}", modelField.fieldName)
                ))
                .reduce(StringBuilder::append)
                .toString();

    }

    private String generateReverseMappings(ModelEntity aEntity) {
        return aEntity.entity.getFields().values().stream()
                .map(ModelField::new)
                .map(modelField -> new StringBuilder(
                        reverseMappingTemplate
                                .replaceAll("\\$\\{propertyGetter\\}", modelField.propertyGetter)
                                .replaceAll("\\$\\{fieldName\\}", modelField.fieldName)
                ))
                .reduce(StringBuilder::append)
                .toString();

    }

    private String generateNavigationProperties(ModelEntity aEntity, Map<String, ModelEntity> modelEntities) {
        return aEntity.references.stream()
                .filter(reference -> modelEntities.containsKey(reference.target))
                .filter(reference -> aEntity.entity.getFields().containsKey(reference.field))
                .map(reference -> {
                    ModelEntity target = modelEntities.get(reference.target);
                    EntityField field = aEntity.entity.getFields().get(reference.field);
                    return new StringBuilder(
                            (field.isNullable() ? nullableNavigationPropertyTemplate : requiredNavigationPropertyTemplate)
                                    .replaceAll("\\$\\{scalarClass\\}", target.className)
                                    .replaceAll("\\$\\{scalarGetter\\}", reference.scalarGetter)
                                    .replaceAll("\\$\\{referenceGetter\\}", reference.referenceGetter)
                                    .replaceAll("\\$\\{scalarModelEntity\\}", target.modelName)
                                    .replaceAll("\\$\\{modelEntity\\}", aEntity.modelName)
                                    .replaceAll("\\$\\{entityKeyGetter\\}", aEntity.keyGetter)
                                    .replaceAll("\\$\\{scalarMutator\\}", reference.scalarMutator)
                                    .replaceAll("\\$\\{collectionGetter\\}", reference.collectionGetter)
                                    .replaceAll("\\$\\{referenceMutator\\}", reference.referenceMutator)
                                    .replaceAll("\\$\\{scalarKeyGetter\\}", target.keyGetter)
                    );
                })
                .reduce(StringBuilder::append)
                .toString();
    }

    private String generateModelEntityBody(ModelEntity aEntity, Map<String, ModelEntity> modelEntities) {
        return modelEntityTemplate
                .replaceAll("\\$\\{entityClass\\}", aEntity.className)
                .replaceAll("\\$\\{navigationProperties\\}", generateNavigationProperties(aEntity, modelEntities))
                .replaceAll("\\$\\{groupsDeclarations\\}", generateGroupsDeclarations(aEntity, modelEntities))
                .replaceAll("\\$\\{forwardMappings\\}", generateForwardMappings(aEntity))
                .replaceAll("\\$\\{reverseMappings\\}", generateReverseMappings(aEntity))
                .replaceAll("\\$\\{groupsFulfills\\}", generateGroupsFulfills(aEntity, modelEntities))
                .replaceAll("\\$\\{entityKeyType\\}", aEntity.keyType)
                .replaceAll("\\$\\{entityQuery\\}", aEntity.modelName)
                .replaceAll("\\$\\{entityRef\\}", aEntity.entity.getName())
                .replaceAll("\\$\\{entityKey\\}", aEntity.keyField.getName())
                .replaceAll("\\$\\{entityKeyGetter\\}", aEntity.keyGetter);
    }

    private String generateModelBody(Map<String, ModelEntity> modelEntities) {
        StringBuilder entitiesRowsImports = modelEntities.values().stream()
                .filter(modelEntity -> !modelEntity.baseClassPackage.isEmpty())
                .map(modelEntity -> new StringBuilder("import " + modelEntity.baseClassPackage + "." + modelEntity.baseClassName + ";" + lf))
                .reduce(StringBuilder::append)
                .orElse(new StringBuilder());
        StringBuilder modelEntitiesBodies = modelEntities.values().stream()
                .map(modelEntity -> generateModelEntityBody(modelEntity, modelEntities))
                .map(StringBuilder::new)
                .reduce(StringBuilder::append)
                .orElse(new StringBuilder());
        StringBuilder modelEntitiesGetters = modelEntities.values().stream()
                .map(modelEntity -> new StringBuilder(modelEntityGetterTemplate
                        .replaceAll("\\$\\{entityKeyType\\}", modelEntity.keyType)
                        .replaceAll("\\$\\{entityClass\\}", modelEntity.className)
                        .replaceAll("\\$\\{entityQueryGetter\\}", "get" + capitalize(modelEntity.modelName))
                        .replaceAll("\\$\\{entityQuery\\}", modelEntity.modelName)
                ))
                .reduce(StringBuilder::append)
                .orElse(new StringBuilder());
        return modelTemplate
                .replaceAll("\\$\\{entitiesRowsImports\\}", entitiesRowsImports.toString())
                .replaceAll("\\$\\{modelEntities\\}", modelEntitiesBodies.toString())
                .replaceAll("\\$\\{modelEntities\\}", modelEntitiesGetters.toString());
    }

    private Map<String, ModelEntity> readModelEntities(JsonNode modelDocument, Path modelPath) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(modelDocument.fields(), 0), false)
                .filter(entityJson -> entityJson.getValue().has("entity") &&
                        entityJson.getValue().get("entity").isTextual() &&
                        !entityJson.getValue().get("entity").asText().isEmpty()
                )
                .map(entityJson -> {
                    String modelEntityName = entityJson.getKey();
                    JsonNode entityBodyNode = entityJson.getValue();
                    String entityRefName = entityBodyNode.get("entity").asText();
                    Path entityPath;
                    if (entityRefName.startsWith("./") || entityRefName.startsWith("../")) {
                        entityPath = modelPath.resolveSibling(entityRefName).normalize();
                    } else {
                        entityPath = entities.getApplicationPath().resolve(entityRefName).normalize();
                    }
                    Path entityRelativePath = entities.getApplicationPath().relativize(entityPath);
                    String entityRelativePathName = entityRelativePath.toString().replace('\\', '/');
                    String entityRef = entityRelativePathName.substring(0, entityRelativePathName.length() - 4);
                    SqlEntity entity = entities.loadEntity(entityRef);
                    JsonNode referencesNode = entityBodyNode.get("references");
                    List<Reference> references = StreamSupport.stream(Spliterators.spliteratorUnknownSize(referencesNode.fields(), 0), false)
                            .filter(referenceJson -> {
                                JsonNode referenceBodyJson = referenceJson.getValue();
                                return referenceBodyJson.has("target") && referenceBodyJson.get("target").isTextual() &&
                                        referenceBodyJson.has("scalar") && referenceBodyJson.get("scalar").isTextual() &&
                                        referenceBodyJson.has("collection") && referenceBodyJson.get("collection").isTextual();
                            })
                            .map(referenceJson -> new Reference(
                                    referenceJson.getKey(),
                                    referenceJson.getValue().get("target").asText(),
                                    referenceJson.getValue().get("scalar").asText(),
                                    referenceJson.getValue().get("collection").asText()
                            ))
                            .filter(reference -> !reference.target.isEmpty() &&
                                    !reference.scalar.isEmpty() &&
                                    !reference.collection.isEmpty()
                            )
                            .collect(Collectors.toList());
                    JsonNode keyNode = entityBodyNode.get("key");
                    Optional<EntityField> keyField;
                    if (keyNode != null && keyNode.isTextual() && !entityBodyNode.asText().isEmpty()) {
                        keyField = Optional.ofNullable(entity.getFields().get(entityBodyNode.asText()));
                    } else {
                        List<EntityField> pks = entity.getFields().values().stream()
                                .filter(Field::isPk)
                                .collect(Collectors.toList());
                        keyField = Optional.ofNullable(pks.size() == 1 ? pks.get(0) : null);
                    }
                    if (!keyField.isPresent()) {
                        throw new IllegalStateException("Entity '" + entityRef + "' doesn't contain key field, or key field is ambiguous");
                    }
                    return new ModelEntity(modelEntityName, entity, keyField.get(), references);
                })
                .collect(Collectors.toMap(modelEntity -> modelEntity.modelName, Function.identity()));
    }

    private static String javaType(EntityField aField) {
        return javaType(aField, false);
    }

    private static String javaType(EntityField aField, boolean aBoxed) {
        if (aField.getType() != null) {
            switch (aField.getType()) {
                case STRING:
                    return "String";
                case LONG:
                    return aField.isNullable() || aBoxed ? "Long" : "long";
                case DOUBLE:
                    return aField.isNullable() || aBoxed ? "Double" : "double";
                case DATE:
                    return "Date";
                case BOOLEAN:
                    return aField.isNullable() || aBoxed ? "Boolean" : "boolean";
                case GEOMETRY:
                    return "String";
                default:
                    return "String";
            }
        } else {
            return "String";
        }
    }

    private static StringBuilder capitalize(String aValue) {
        return Stream.of(aValue.split("_+"))
                .map(part -> new StringBuilder(part.substring(0, 1).toUpperCase() + part.substring(1)))
                .reduce(StringBuilder::append)
                .orElse(new StringBuilder());
    }

    private static String sanitize(String aValue) {
        return aValue.replaceAll("[^0-9a-zA-Z_]", "_");
    }

    private static String accessor(EntityField field) {
        return sanitizeCapitalize(field.getName());
    }

    private static String entityClass(String name) {
        return sanitizeCapitalize(name);
    }

    private static String sanitizeCapitalize(String name) {
        return capitalize(sanitize(name)).toString();
    }

    private static String entityRowClass(String name) {
        return entityClass(name) + "Row";
    }

    private static String mutatorArg(EntityField field) {
        return "a" + accessor(field);
    }
}
