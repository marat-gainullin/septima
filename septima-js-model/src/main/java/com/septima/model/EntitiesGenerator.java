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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    private final String groupEvictTemplate;
    private final String requiredScalarPropertyTemplate;
    private final String nullableScalarPropertyTemplate;
    private final String collectionPropertyTemplate;

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
                loadResource("/model.template", aCharset),
                loadResource("/model-entity.template", aCharset),
                loadResource("/model-entity-getter.template", aCharset),
                loadResource("/model-entity/forward-mapping.template", aCharset),
                loadResource("/model-entity/reverse-mapping.template", aCharset),
                loadResource("/model-entity/group-declaration.template", aCharset),
                loadResource("/model-entity/group-fulfill.template", aCharset),
                loadResource("/model-entity/group-evict.template", aCharset),
                loadResource("/model-entity/required-scalar-property.template", aCharset),
                loadResource("/model-entity/nullable-scalar-property.template", aCharset),
                loadResource("/model-entity/collection-property.template", aCharset),
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
                             String aGroupEvictTemplate,
                             String aRequiredScalarPropertyTemplate,
                             String aNullableScalarPropertyTemplate,
                             String aCollectionPropertyTemplate,
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
        groupEvictTemplate = aGroupEvictTemplate;
        requiredScalarPropertyTemplate = aRequiredScalarPropertyTemplate;
        nullableScalarPropertyTemplate = aNullableScalarPropertyTemplate;
        collectionPropertyTemplate = aCollectionPropertyTemplate;
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
            String accessor = toPascalCase(field.getName());
            property = accessor.substring(0, 1).toLowerCase() + accessor.substring(1);
            propertyGetter = "get" + accessor;
            propertyMutator = "set" + accessor;
            mutatorArg = mutatorArg(field.getName());
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
                        String entityRowClassName = entityRowClass(entity.getName().substring(entity.getName().lastIndexOf('/') + 1));
                        String entityRow = (entityRelativeDirPath != null ? ("package " + entityRelativeDirPath.toString().replace('\\', '/').replace('/', '.') + ";" + lf + lf) : "")
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
                        Path entityClassFile = destination.resolve(entityRelativePath.resolveSibling(entityRowClassName + ".java"));
                        if (!entityClassFile.getParent().toFile().exists()) {
                            entityClassFile.getParent().toFile().mkdirs();
                        }
                        Files.write(entityClassFile, entityRow.getBytes(charset));
                        Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.INFO, "Sql entity '" + entityRef + "' transformed and written to: " + entityClassFile);
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
                        String modelRelativePathName = modelRelativePath.getFileName().toString();
                        String modelClassName = toPascalCase(modelRelativePathName.substring(0, modelRelativePathName.length() - 11));
                        ObjectMapper jsonMapper = new ObjectMapper();
                        JsonNode modelDocument = jsonMapper.readTree(modelPath.toFile());
                        if (modelDocument != null && modelDocument.isObject()) {
                            Map<String, ModelEntity> modelEntities = readModelEntities(modelDocument, modelPath);
                            complementReferences(modelEntities);
                            resolveInReferences(modelEntities);
                            String modelBody = (modelRelativeDirPath != null ? ("package " + modelRelativeDirPath.toString().replace('\\', '/').replace('/', '.') + ";" + lf + lf) : "")
                                    + generateModelBody(modelEntities, modelClassName);
                            Path modelClassFile = destination.resolve(modelRelativePath.resolveSibling(modelClassName + ".java"));
                            if (!modelClassFile.getParent().toFile().exists()) {
                                modelClassFile.getParent().toFile().mkdirs();
                            }
                            Files.write(modelClassFile, modelBody.getBytes(charset));
                            Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.INFO, "Model definition '" + modelPath + "' transformed and written to: " + modelClassFile);
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

    private class Reference {
        private final String property;
        private final String type;
        private final String source;
        private final String destination;
        private final String scalar;
        private final String scalarGetter;
        private final String scalarMutator;
        private final String collectionGetter;
        private final String collection;
        private final String getter;
        private final String mutator;

        private Reference(String aProperty, String aType, String aSource, String aTarget, String aScalar, String aCollection) {
            property = aProperty;
            type = aType;
            String accessor = property.substring(0, 1).toUpperCase() + property.substring(1);
            getter = "get" + accessor;
            mutator = "set" + accessor;
            source = aSource;
            destination = aTarget;
            scalar = aScalar;
            scalarGetter = "get" + toPascalCase(scalar);
            scalarMutator = "set" + toPascalCase(scalar);
            collection = aCollection;
            collectionGetter = !collection.isEmpty() ? "get" + toPascalCase(collection) : null;
        }
    }

    private class ModelEntity {
        private final String modelName;
        private final Map<String, EntityField> fieldsByProperty;
        private final Map<String, Reference> inReferences = new HashMap<>();
        private final Map<String, Reference> outReferences;
        private final SqlEntity entity;
        private final String key;
        private final String keyType;
        private final String boxedKeyType;
        private final String keyGetter;
        private final String keyMutator;
        private final String className;
        private final String baseClassName;
        private final String baseClassPackage;

        private ModelEntity(
                String aModelName,
                String aClassName,
                SqlEntity aEntity,
                String aKey,
                String aKeyType,
                String aKeyBoxedType,
                Map<String, EntityField> aFieldsByProperty,
                Map<String, Reference> aReferences
        ) {
            modelName = aModelName;
            outReferences = aReferences;
            entity = aEntity;

            fieldsByProperty = aFieldsByProperty;
            key = aKey;
            keyType = aKeyType;
            boxedKeyType = aKeyBoxedType;
            String keyAccessor = toPascalCase(aKey);

            keyGetter = "get" + keyAccessor;
            keyMutator = "set" + keyAccessor;
            if (aClassName != null && !aClassName.isEmpty()) {
                className = aClassName;
            } else {
                String pascalModelName = toPascalCase(modelName);
                if (pascalModelName.length() > 1 && pascalModelName.endsWith("s")) {
                    className = pascalModelName.substring(0, pascalModelName.length() - 1);
                } else {
                    className = pascalModelName;
                }
            }
            String entityRef = entity.getName();
            int lastSlashAt = entityRef.lastIndexOf('/');
            baseClassName = entityRowClass(entityRef.substring(lastSlashAt + 1));
            baseClassPackage = lastSlashAt > -1 ? entityRef.substring(0, lastSlashAt).replace('/', '.') : null;
        }
    }

    private String generateGroupsDeclarations(ModelEntity aEntity) {
        return aEntity.outReferences.values().stream()
                .map(reference -> replaceVariables(
                        groupDeclarationTemplate, Map.of(
                                "entityKeyType", aEntity.boxedKeyType,
                                "entityClass", aEntity.className,
                                "modelEntity", aEntity.modelName,
                                "Reference", reference.getter.substring(3)
                        )
                ))
                .reduce(StringBuilder::append)
                .map(r -> new StringBuilder(lf).append(r))
                .orElse(new StringBuilder())
                .toString();
    }

    private String generateGroupsFulfills(ModelEntity aEntity) {
        return aEntity.outReferences.values().stream()
                .map(reference -> replaceVariables(
                        groupFulfillTemplate, Map.of(
                                "modelEntity", aEntity.modelName,
                                "Reference", reference.getter.substring(3),
                                "referenceGetter", reference.getter
                        )
                ))
                .reduce((g1, g2) -> g1.append(lf).append(g2))
                .map(r -> new StringBuilder(lf).append(r))
                .orElse(new StringBuilder())
                .toString();
    }

    private String generateGroupsEvicts(ModelEntity aEntity) {
        return aEntity.outReferences.values().stream()
                .map(reference -> replaceVariables(
                        groupEvictTemplate, Map.of(
                                "modelEntity", aEntity.modelName,
                                "Reference", reference.getter.substring(3),
                                "referenceGetter", reference.getter
                        )
                ))
                .reduce((g1, g2) -> g1.append(lf).append(g2))
                .map(r -> new StringBuilder(lf).append(r))
                .orElse(new StringBuilder())
                .toString();
    }

    private String generateForwardMappings(ModelEntity aEntity) {
        return aEntity.entity.getFields().values().stream()
                .map(ModelField::new)
                .map(modelField -> replaceVariables(
                        forwardMappingTemplate, Map.of(
                                "propertyMutator", modelField.propertyMutator,
                                "propertyType", modelField.propertyType,
                                "fieldName", modelField.fieldName
                        )
                ))
                .reduce(StringBuilder::append)
                .orElse(new StringBuilder())
                .toString();

    }

    private String generateReverseMappings(ModelEntity aEntity) {
        return aEntity.entity.getFields().values().stream()
                .map(ModelField::new)
                .map(modelField -> replaceVariables(
                        reverseMappingTemplate, Map.of(
                                "propertyGetter", modelField.propertyGetter,
                                "fieldName", modelField.fieldName
                        )
                ))
                .reduce((m1, m2) -> m1.append(",").append(lf).append(m2))
                .orElse(new StringBuilder())
                .toString();
    }

    private String generateScalarProperties(ModelEntity aEntity, Map<String, ModelEntity> modelEntities) {
        return aEntity.outReferences.values().stream()
                .filter(reference -> {
                    if (!modelEntities.containsKey(reference.destination)) {
                        Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.WARNING, "Target model entity '" + reference.destination + "' is not found in model while scalar property '" + aEntity.modelName + "." + reference.scalar + "' generation");
                    }
                    return modelEntities.containsKey(reference.destination);
                })
                .filter(reference -> {
                    if (!aEntity.fieldsByProperty.containsKey(reference.property)) {
                        Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.WARNING, "No reference property '" + reference.property + "' found in model entity '" + aEntity.modelName + "' while scalar property '" + aEntity.modelName + "." + reference.scalar + "' generation");
                    }
                    return aEntity.fieldsByProperty.containsKey(reference.property);
                })
                .map(reference -> {
                    ModelEntity target = modelEntities.get(reference.destination);
                    Field field = aEntity.fieldsByProperty.get(reference.property);
                    return replaceVariables(
                            (field.isNullable() ? nullableScalarPropertyTemplate : requiredScalarPropertyTemplate), Map.ofEntries(
                                    Map.entry("scalarClass", target.className),
                                    Map.entry("scalarGetter", reference.scalarGetter),
                                    Map.entry("referenceGetter", reference.getter),
                                    Map.entry("Reference", reference.getter.substring(3)),
                                    Map.entry("referenceType", reference.type),
                                    Map.entry("scalarModelEntity", target.modelName),
                                    Map.entry("modelEntity", aEntity.modelName),
                                    Map.entry("entityKeyGetter", aEntity.keyGetter),
                                    Map.entry("scalarMutator", reference.scalarMutator),
                                    Map.entry("referenceMutator", reference.mutator),
                                    Map.entry("scalarKeyGetter", target.keyGetter)
                            )
                    );
                })
                .reduce((p1, p2) -> p1.append(lf).append(p2))
                .map(r -> new StringBuilder(lf).append(r))
                .orElse(new StringBuilder())
                .toString();
    }

    private String generateCollectionProperties(ModelEntity aEntity, Map<String, ModelEntity> modelEntities) {
        return aEntity.inReferences.values().stream()
                .map(reference -> {
                    ModelEntity sourceEntity = modelEntities.get(reference.source);
                    return replaceVariables(collectionPropertyTemplate, Map.of(
                            "scalarClass", sourceEntity.className,
                            "collectionGetter", reference.collectionGetter,
                            "sourceModelEntity", sourceEntity.modelName,
                            "Reference", reference.getter.substring(3),
                            "entityKeyGetter", aEntity.keyGetter
                    ));
                })
                .reduce(StringBuilder::append)
                .map(r -> new StringBuilder(lf).append(r))
                .orElse(new StringBuilder())
                .toString();
    }

    private String generateModelEntityBody(ModelEntity aEntity, Map<String, ModelEntity> modelEntities) {
        return replaceVariables(modelEntityTemplate, Map.ofEntries(
                Map.entry("entityClass", aEntity.className),
                Map.entry("entityBaseClass", aEntity.baseClassName),
                Map.entry("scalarProperties", generateScalarProperties(aEntity, modelEntities)),
                Map.entry("collectionProperties", generateCollectionProperties(aEntity, modelEntities)),
                Map.entry("groupsDeclarations", generateGroupsDeclarations(aEntity)),
                Map.entry("forwardMappings", generateForwardMappings(aEntity)),
                Map.entry("reverseMappings", generateReverseMappings(aEntity)),
                Map.entry("groupsFulfills", generateGroupsFulfills(aEntity)),
                Map.entry("groupsEvicts", generateGroupsEvicts(aEntity)),
                Map.entry("entityKeyType", aEntity.keyType),
                Map.entry("entityKeyBoxedType", aEntity.boxedKeyType),
                Map.entry("modelEntity", aEntity.modelName),
                Map.entry("entityRef", aEntity.entity.getName()),
                Map.entry("entityKey", aEntity.key),
                Map.entry("entityKeyGetter", aEntity.keyGetter),
                Map.entry("entityKeyMutator", aEntity.keyMutator)
        ))
                .toString();
    }

    private String generateModelBody(Map<String, ModelEntity> modelEntities, String modelClassName) {
        StringBuilder entitiesRowsImports = modelEntities.values().stream()
                .filter(modelEntity -> modelEntity.baseClassPackage != null && !modelEntity.baseClassPackage.isEmpty())
                .map(modelEntity -> "import " + modelEntity.baseClassPackage + "." + modelEntity.baseClassName + ";" + lf)
                .distinct()
                .map(StringBuilder::new)
                .reduce(StringBuilder::append)
                .orElse(new StringBuilder());
        StringBuilder modelEntitiesBodies = modelEntities.values().stream()
                .map(modelEntity -> generateModelEntityBody(modelEntity, modelEntities))
                .map(StringBuilder::new)
                .reduce(StringBuilder::append)
                .orElse(new StringBuilder());
        StringBuilder modelEntitiesGetters = modelEntities.values().stream()
                .map(modelEntity -> replaceVariables(modelEntityGetterTemplate, Map.of(
                        "entityKeyType", modelEntity.boxedKeyType,
                        "entityClass", modelEntity.className,
                        "modelEntityGetter", "get" + toPascalCase(modelEntity.modelName),
                        "modelEntity", modelEntity.modelName
                )))
                .reduce((eg1, eg2) -> eg1.append(lf).append(eg2))
                .orElse(new StringBuilder());
        return replaceVariables(modelTemplate, Map.of(
                "modelClass", modelClassName,
                "entitiesRowsImports", entitiesRowsImports.toString(),
                "modelEntities", modelEntitiesBodies.toString(),
                "modelEntitiesGetters", modelEntitiesGetters.toString()
        ))
                .toString();
    }

    private void complementReferences(Map<String, ModelEntity> aEntities) {
        Map<String, Set<ModelEntity>> byQualifiedTableName = aEntities.values().stream()
                .flatMap(modelEntity -> modelEntity.entity.getFields().values().stream()
                        .map(field -> Map.entry(field.getTableName(), modelEntity)))
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toSet())));
        aEntities.values()
                .forEach(sourceEntity -> sourceEntity.fieldsByProperty.entrySet().stream()
                        .filter(e -> e.getValue().isFk())
                        .filter(e -> !sourceEntity.outReferences.containsKey(e.getKey()))
                        .forEach(e -> {
                            String propertyName = e.getKey();
                            EntityField propertyField = e.getValue();
                            if (propertyName.endsWith("Id")) {
                                String scalarName = propertyName.substring(0, propertyName.length() - 2);
                                Collection<ModelEntity> targetEntities = byQualifiedTableName.getOrDefault(e.getValue().getFk().getReferee().getTable(), Set.of());
                                if (targetEntities.size() == 1) {
                                    ModelEntity targetEntity = targetEntities.iterator().next();
                                    if (!targetEntity.fieldsByProperty.containsKey(sourceEntity.modelName)) {
                                        sourceEntity.outReferences.put(propertyName, new Reference(propertyName, javaType(propertyField), sourceEntity.modelName, targetEntity.modelName, scalarName, sourceEntity.modelName));
                                    } else {
                                        Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.WARNING, "Generated collection property name clashes with original property '" + targetEntity.modelName + "." + sourceEntity.modelName + "'");
                                    }
                                } else if (targetEntities.isEmpty()) {
                                    Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.WARNING, "No target model entity found for scalar property '" + sourceEntity.modelName + "." + scalarName + "' while references auto detection");
                                } else {
                                    Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.WARNING, "Target model entity for scalar property '" + sourceEntity.modelName + "." + scalarName + "' is ambiguous. Candidates are: [" + targetEntities.stream()
                                            .map(modelEntity -> modelEntity.modelName)
                                            .map(name -> new StringBuilder().append("'").append(name).append("'"))
                                            .reduce((name1, name2) -> name1.append(", ").append(name2))
                                            .get()
                                            .toString() + "]");
                                }
                            } else {
                                Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.WARNING, "Property '" + propertyName + "' is not suitable for scalar property name generation in model entity '" + sourceEntity.modelName);
                            }
                        })
                );
    }

    private static void resolveInReferences(Map<String, ModelEntity> aEntities) {
        aEntities.values().stream()
                .flatMap(modelEntity -> modelEntity.outReferences.values().stream())
                .forEach(reference -> {
                    if (aEntities.containsKey(reference.destination)) {
                        aEntities.get(reference.destination).inReferences.put(reference.collection, reference);
                    } else {
                        Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.WARNING, "Target model entity '" + reference.destination + "' is not found in reference '" + reference.source + "." + reference.property + "'");
                    }
                });
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
                    String entityRef = entityRelativePath.toString().replace('\\', '/');
                    SqlEntity entity = entities.loadEntity(entityRef);

                    Map<String, EntityField> fieldsByProperty = entity.getFields().values().stream()
                            .collect(Collectors.toMap(field -> fieldToProperty(field.getName()), Function.identity()));

                    JsonNode referencesNode = entityBodyNode.get("references");
                    Map<String, Reference> references = referencesNode != null && referencesNode.isObject() ?
                            StreamSupport.stream(Spliterators.spliteratorUnknownSize(referencesNode.fields(), 0), false)
                                    .filter(referenceJson -> {
                                        JsonNode referenceBodyJson = referenceJson.getValue();
                                        return referenceBodyJson.has("target") && referenceBodyJson.get("target").isTextual() &&
                                                referenceBodyJson.has("scalar") && referenceBodyJson.get("scalar").isTextual() &&
                                                referenceBodyJson.has("collection") && referenceBodyJson.get("collection").isTextual();
                                    })
                                    .filter(referenceJson -> fieldsByProperty.containsKey(fieldToProperty(referenceJson.getKey())))
                                    .map(referenceJson -> new Reference(
                                            fieldToProperty(referenceJson.getKey()),
                                            javaType(fieldsByProperty.get(fieldToProperty(referenceJson.getKey()))),
                                            modelEntityName,
                                            referenceJson.getValue().get("target").asText(),
                                            referenceJson.getValue().get("scalar").asText(),
                                            referenceJson.getValue().get("collection").asText()
                                    ))
                                    .filter(reference -> !reference.destination.isEmpty() &&
                                            !reference.scalar.isEmpty() &&
                                            !reference.collection.isEmpty()
                                    )
                                    .collect(Collectors.toMap(reference -> reference.property, Function.identity())) :
                            new HashMap<>();
                    JsonNode keyNode = entityBodyNode.get("key");
                    String keyName;
                    EntityField keyField;
                    if (keyNode != null && keyNode.isTextual() && !keyNode.asText().isEmpty()) {
                        keyName = fieldToProperty(keyNode.asText());
                        keyField = fieldsByProperty.get(keyName);
                    } else {
                        List<EntityField> pks = fieldsByProperty.values().stream()
                                .filter(Field::isPk)
                                .collect(Collectors.toList());
                        if (pks.size() == 1) {
                            keyField = pks.get(0);
                            keyName = fieldToProperty(keyField.getName());
                        } else if (pks.isEmpty()) {
                            keyName = null;
                            keyField = null;
                        } else {
                            throw new IllegalStateException("Model entity '" + modelEntityName + "' in model '" + modelPath + "' has ambiguous key. Candidates are: [" + pks.stream()
                                    .map(field -> fieldToProperty(field.getName()))
                                    .map(pkName -> new StringBuilder("'").append(pkName).append("'"))
                                    .reduce((pkName1, pkName2) -> pkName1.append(", ").append(pkName2))
                                    .get()
                                    .toString()
                                    + "]"
                            );
                        }
                    }
                    if (keyField != null) {
                        JsonNode classNameNode = entityBodyNode.get("className");
                        String modelClassName = classNameNode != null && classNameNode.isTextual() ? classNameNode.asText(null) : null;
                        return new ModelEntity(
                                modelEntityName,
                                modelClassName,
                                entity,
                                keyName,
                                javaType(keyField),
                                javaType(keyField, true),
                                fieldsByProperty,
                                references);
                    } else {
                        throw new IllegalStateException("Model entity '" + modelEntityName + "' in model '" + modelPath + "' doesn't contain key field");
                    }
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

    private static String toPascalCase(String name) {
        return Stream.of(name.replaceAll("[^0-9a-zA-Z_]", "_").split("_+"))
                .map(part -> new StringBuilder(part.substring(0, 1).toUpperCase() + part.substring(1)))
                .reduce(StringBuilder::append)
                .orElse(new StringBuilder())
                .toString();
    }

    private static String entityRowClass(String name) {
        return toPascalCase(name) + "Row";
    }

    private static String mutatorArg(String field) {
        return "a" + toPascalCase(field);
    }

    /**
     * Transforms name like {@code customer_id} into name like {@code customerId}
     * It is idempotent. So if model's references are declared under names {@code customer_id} or {@code customerId} in *.model.json, - they are equivalent.
     * Warning! If such declarations are present in both forms, only one will survive.
     *
     * @param aFieldName A name like {@code customer_id}.
     * @return A name like {@code customerId}.
     */
    private static String fieldToProperty(String aFieldName) {
        String accessor = toPascalCase(aFieldName);
        return accessor.substring(0, 1).toLowerCase() + accessor.substring(1);
    }

    private static String loadResource(String resourceName, Charset aCharset) throws IOException, URISyntaxException {
        return new String(Files.readAllBytes(Paths.get(EntitiesGenerator.class.getResource(resourceName).toURI())), aCharset);
    }

    private static Pattern VAR_PATTERN = Pattern.compile("\\$\\{([a-zA-Z0-9_]+)\\}");

    private StringBuilder replaceVariables(String aBody, Map<String, String> aVariables) {
        StringBuilder body = new StringBuilder();
        Matcher matcher = VAR_PATTERN.matcher(aBody);
        while (matcher.find()) {
            if (!aVariables.containsKey(matcher.group(1))) {
                throw new IllegalStateException("Unbound variable '" + matcher.group(1) + "' in template:" + lf + aBody);
            }
            matcher.appendReplacement(body, aVariables.get(matcher.group(1)));
        }
        matcher.appendTail(body);
        return body;
    }

}
