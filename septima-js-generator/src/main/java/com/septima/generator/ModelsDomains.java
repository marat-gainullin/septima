package com.septima.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import com.septima.metadata.EntityField;
import com.septima.metadata.Field;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ModelsDomains extends EntitiesProcessor {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String modelTemplate;
    private final String modelEntityGetterTemplate;
    private final String modelInstanceFactoryTemplate;
    private final String modelEntityTemplate;
    private final String groupDeclarationTemplate;
    private final String groupFulfillTemplate;
    private final String groupEvictTemplate;
    private final String requiredScalarPropertyTemplate;
    private final String nullableScalarPropertyTemplate;
    private final String collectionPropertyTemplate;

    private final Path modelsRoot;

    public static ModelsDomains fromResources(SqlEntities anEntities, Path aModelsRoot, Path aDestination) throws IOException {
        return fromResources(anEntities, aModelsRoot, aDestination, StandardCharsets.UTF_8, System.lineSeparator());
    }

    public static ModelsDomains fromResources(SqlEntities anEntities, Path aModelsRoot, Path aDestination, Charset aCharset, String aLf) throws IOException {
        return new ModelsDomains(anEntities, aModelsRoot, aDestination,
                Utils.loadResource("/model.template", aCharset, aLf),
                Utils.loadResource("/model-entity.template", aCharset, aLf),
                Utils.loadResource("/model-entity-getter.template", aCharset, aLf),
                Utils.loadResource("/model-instance-factory.template", aCharset, aLf),
                Utils.loadResource("/model-entity/group-declaration.template", aCharset, aLf),
                Utils.loadResource("/model-entity/group-fulfill.template", aCharset, aLf),
                Utils.loadResource("/model-entity/group-evict.template", aCharset, aLf),
                Utils.loadResource("/model-entity/required-scalar-property.template", aCharset, aLf),
                Utils.loadResource("/model-entity/nullable-scalar-property.template", aCharset, aLf),
                Utils.loadResource("/model-entity/collection-property.template", aCharset, aLf),
                aLf, aCharset);
    }

    private ModelsDomains(SqlEntities anEntities, Path aModelsRoot, Path aDestination,
                          String aModelTemplate,
                          String aModelEntityTemplate,
                          String aModelEntityGetterTemplate,
                          String aModelInstanceFactoryTemplate,
                          String aGroupDeclarationTemplate,
                          String aGroupFulfillTemplate,
                          String aGroupEvictTemplate,
                          String aRequiredScalarPropertyTemplate,
                          String aNullableScalarPropertyTemplate,
                          String aCollectionPropertyTemplate,
                          String aLf, Charset aCharset) {
        super(anEntities, aDestination, aLf, aCharset);
        modelsRoot = aModelsRoot;
        modelTemplate = aModelTemplate;
        modelEntityTemplate = aModelEntityTemplate;
        modelEntityGetterTemplate = aModelEntityGetterTemplate;
        modelInstanceFactoryTemplate = aModelInstanceFactoryTemplate;
        groupDeclarationTemplate = aGroupDeclarationTemplate;
        groupFulfillTemplate = aGroupFulfillTemplate.replace(aLf, "");
        groupEvictTemplate = aGroupEvictTemplate.replace(aLf, "");
        requiredScalarPropertyTemplate = aRequiredScalarPropertyTemplate;
        nullableScalarPropertyTemplate = aNullableScalarPropertyTemplate;
        collectionPropertyTemplate = aCollectionPropertyTemplate;
    }

    public Path considerJavaSource(Path modelDefinition) {
        Path modelRelativePath = modelsRoot.relativize(modelDefinition).normalize();
        String modelName = modelRelativePath.getFileName().toString();
        String modelClassName = Utils.toPascalCase(modelName.substring(0, modelName.length() - 11));
        return destination.resolve(modelRelativePath.resolveSibling(modelClassName + ".java"));
    }

    public Path toJavaSource(Path modelPath) throws IOException {
        Path modelRelativePath = modelsRoot.relativize(modelPath).normalize();
        Path modelRelativeDirPath = modelRelativePath.getParent();
        String modelName = modelRelativePath.getFileName().toString();
        String domainClassName = Utils.toPascalCase(modelName.substring(0, modelName.length() - 11));
        JsonNode modelDocument = JSON.readTree(modelPath.toFile());
        if (modelDocument != null && modelDocument.isObject()) {
            Map<String, ModelEntity> modelEntities = readModelEntities(modelDocument, modelPath);
            complementReferences(modelEntities);
            resolveInReferences(modelEntities);
            String modelBody = generateModelBody(modelEntities, domainClassName, modelRelativeDirPath != null ? ("package " + modelRelativeDirPath.toString().replace('\\', '/').replace('/', '.') + ";" + lf + lf) : "");
            Path modelClassFile = destination.resolve(modelRelativePath.resolveSibling(domainClassName + ".java"));
            if (!modelClassFile.getParent().toFile().exists()) {
                modelClassFile.getParent().toFile().mkdirs();
            }
            Files.write(modelClassFile, modelBody.getBytes(charset));
            Logger.getLogger(ModelsDomains.class.getName()).log(Level.INFO, "Model definition '" + modelPath + "' transformed and written to: " + modelClassFile);
            return modelClassFile;
        } else {
            throw new IllegalArgumentException("Bad '*.model.json' format detected in '" + modelPath + "'. 'sqlEntities' is required object in model definition");
        }
    }

    public int deepToJavaSources(Path source) throws IOException {
        return Files.walk(source)
                .filter(path -> path.toString().toLowerCase().endsWith(".model.json"))
                .map(modelPath -> {
                    try {
                        toJavaSource(modelPath);
                        return 1;
                    } catch (IOException | IllegalArgumentException ex) {
                        Logger.getLogger(ModelsDomains.class.getName()).log(Level.SEVERE, "Model definition '" + modelPath + "' skipped due to an exception", ex);
                        return 0;
                    }
                })
                .reduce(Integer::sum)
                .orElse(0);
    }

    private static class Reference {
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
            scalarGetter = "get" + Utils.toPascalCase(scalar);
            scalarMutator = "set" + Utils.toPascalCase(scalar);
            collection = aCollection;
            collectionGetter = !collection.isEmpty() ? "get" + Utils.toPascalCase(collection) : null;
        }
    }

    private static class ModelEntity {
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
            String keyAccessor = Utils.toPascalCase(aKey);

            keyGetter = "get" + keyAccessor;
            keyMutator = "set" + keyAccessor;
            if (aClassName != null && !aClassName.isEmpty()) {
                className = aClassName;
            } else {
                String pascalModelName = Utils.toPascalCase(modelName);
                if (pascalModelName.length() > 1 && pascalModelName.endsWith("s")) {
                    className = pascalModelName.substring(0, pascalModelName.length() - 1);
                } else {
                    className = pascalModelName;
                }
            }
            String entityRef = entity.getName();
            int lastSlashAt = entityRef.lastIndexOf('/');
            baseClassName = Utils.rawClass(entityRef.substring(lastSlashAt + 1));
            baseClassPackage = lastSlashAt > -1 ? entityRef.substring(0, lastSlashAt).replace('/', '.') : null;
        }
    }

    private String generateGroupsDeclarations(ModelEntity aEntity) {
        return aEntity.outReferences.values().stream()
                .map(reference -> Utils.replaceVariables(groupDeclarationTemplate, Map.of(
                        "entityKeyType", aEntity.boxedKeyType,
                        "entityClass", aEntity.className,
                        "modelEntity", aEntity.modelName,
                        "Reference", reference.getter.substring(3)
                ), lf))
                .reduce(StringBuilder::append)
                .map(r -> new StringBuilder(lf).append(r))
                .orElse(new StringBuilder())
                .toString();
    }

    private String generateGroupsFulfills(ModelEntity aEntity) {
        return aEntity.outReferences.values().stream()
                .map(reference -> Utils.replaceVariables(groupFulfillTemplate, Map.of(
                        "modelEntity", aEntity.modelName,
                        "Reference", reference.getter.substring(3),
                        "referenceGetter", reference.getter
                ), lf))
                .reduce((g1, g2) -> g1.append(lf).append(g2))
                .map(r -> new StringBuilder(lf).append(r))
                .orElse(new StringBuilder())
                .toString();
    }

    private String generateGroupsEvicts(ModelEntity aEntity) {
        return aEntity.outReferences.values().stream()
                .map(reference -> Utils.replaceVariables(groupEvictTemplate, Map.of(
                        "modelEntity", aEntity.modelName,
                        "Reference", reference.getter.substring(3),
                        "referenceGetter", reference.getter
                ), lf))
                .reduce((g1, g2) -> g1.append(lf).append(g2))
                .map(r -> new StringBuilder(lf).append(r))
                .orElse(new StringBuilder())
                .toString();
    }

    private String generateScalarProperties(ModelEntity aEntity, Map<String, ModelEntity> modelEntities) {
        return aEntity.outReferences.values().stream()
                .filter(reference -> {
                    if (!modelEntities.containsKey(reference.destination)) {
                        Logger.getLogger(ModelsDomains.class.getName()).log(Level.WARNING, "Target model entity '" + reference.destination + "' is not found in model while scalar property '" + aEntity.modelName + "." + reference.scalar + "' generation");
                    }
                    return modelEntities.containsKey(reference.destination);
                })
                .filter(reference -> {
                    Field field = aEntity.fieldsByProperty.get(reference.property);
                    if (field == null) {
                        Logger.getLogger(ModelsDomains.class.getName()).log(Level.WARNING, "No base field '" + reference.property + "' found in model entity '" + aEntity.modelName + "' while scalar property '" + aEntity.modelName + "." + reference.scalar + "' generation");
                    } else if (field.isPk()){
                        Logger.getLogger(ModelsDomains.class.getName()).log(Level.WARNING, "Reference property '" + reference.property + "' in model entity '" + aEntity.modelName + "' ignored while scalar property '" + aEntity.modelName + "." + reference.scalar + "' generation. Scalar properties based on primary keys are not supported.");
                    }
                    return field != null && !field.isPk(); // If the field is a PK and FK at the same time, we ignore its scalar reference. So have to filter it out here.
                })
                .map(reference -> {
                    ModelEntity target = modelEntities.get(reference.destination);
                    Field field = aEntity.fieldsByProperty.get(reference.property);
                    return Utils.replaceVariables(
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
                            ), lf
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
                    return Utils.replaceVariables(collectionPropertyTemplate, Map.of(
                            "scalarClass", sourceEntity.className,
                            "collectionGetter", reference.collectionGetter,
                            "sourceModelEntity", sourceEntity.modelName,
                            "Reference", reference.getter.substring(3),
                            "entityKeyGetter", aEntity.keyGetter
                    ), lf);
                })
                .reduce(StringBuilder::append)
                .map(r -> new StringBuilder(lf).append(r))
                .orElse(new StringBuilder())
                .toString();
    }

    private String generateModelEntityBody(ModelEntity anEntity, Map<String, ModelEntity> modelEntities) {
        return Utils.replaceVariables(modelEntityTemplate, Map.ofEntries(
                Map.entry("entityClass", anEntity.className),
                Map.entry("entityBaseClass", anEntity.baseClassName),
                Map.entry("scalarProperties", generateScalarProperties(anEntity, modelEntities)),
                Map.entry("collectionProperties", generateCollectionProperties(anEntity, modelEntities)),
                Map.entry("groupsDeclarations", generateGroupsDeclarations(anEntity)),
                Map.entry("groupsFulfills", generateGroupsFulfills(anEntity)),
                Map.entry("groupsEvicts", generateGroupsEvicts(anEntity)),
                Map.entry("entityKeyType", anEntity.keyType),
                Map.entry("entityKeyBoxedType", anEntity.boxedKeyType),
                Map.entry("modelEntity", anEntity.modelName),
                Map.entry("entityRef", anEntity.entity.getName()),
                Map.entry("entityKey", anEntity.key),
                Map.entry("entityKeyGetter", anEntity.keyGetter),
                Map.entry("entityKeyMutator", anEntity.keyMutator)
        ), lf).toString();
    }

    private String generateModelBody(Map<String, ModelEntity> modelEntities, String modelClassName, String packageName) {
        StringBuilder rawsImports = modelEntities.values().stream()
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
                .map(modelEntity -> Utils.replaceVariables(modelEntityGetterTemplate, Map.of(
                        "entityKeyType", modelEntity.boxedKeyType,
                        "entityClass", modelEntity.className,
                        "modelEntityGetter", "get" + Utils.toPascalCase(modelEntity.modelName),
                        "modelEntity", modelEntity.modelName
                ), lf))
                .reduce((eg1, eg2) -> eg1.append(lf).append(eg2))
                .orElse(new StringBuilder());
        StringBuilder modelInstancesFactories = modelEntities.values().stream()
                .map(modelEntity -> Utils.replaceVariables(modelInstanceFactoryTemplate, Map.of(
                        "entityClass", modelEntity.className
                ), lf))
                .reduce((eg1, eg2) -> eg1.append(lf).append(eg2))
                .orElse(new StringBuilder());
        return Utils.replaceVariables(modelTemplate, Map.of(
                "package", packageName,
                "modelClass", modelClassName,
                "entitiesRowsImports", rawsImports.toString(),
                "modelEntities", modelEntitiesBodies.toString(),
                "modelEntitiesGetters", modelEntitiesGetters.toString(),
                "modelInstancesFactories", modelInstancesFactories.toString()
        ), lf).toString();
    }

    private void complementReferences(Map<String, ModelEntity> aEntities) {
        Map<String, Set<ModelEntity>> byQualifiedTableName = aEntities.values().stream()
                .flatMap(modelEntity -> modelEntity.entity.getFields().values().stream()
                        .filter(field -> field.getTableName() != null)
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
                                        sourceEntity.outReferences.put(propertyName, new Reference(propertyName, Utils.javaType(propertyField), sourceEntity.modelName, targetEntity.modelName, scalarName, sourceEntity.modelName));
                                    } else {
                                        Logger.getLogger(ModelsDomains.class.getName()).log(Level.WARNING, "Generated collection property name clashes with original property '" + targetEntity.modelName + "." + sourceEntity.modelName + "'");
                                    }
                                } else if (targetEntities.isEmpty()) {
                                    Logger.getLogger(ModelsDomains.class.getName()).log(Level.WARNING, "No target model entity found for scalar property '" + sourceEntity.modelName + "." + scalarName + "' while references auto detection");
                                } else {
                                    Logger.getLogger(ModelsDomains.class.getName()).log(Level.WARNING, "Target model entity for scalar property '" + sourceEntity.modelName + "." + scalarName + "' is ambiguous. Candidates are: [" + targetEntities.stream()
                                            .map(modelEntity -> modelEntity.modelName)
                                            .map(name -> new StringBuilder().append("'").append(name).append("'"))
                                            .reduce((name1, name2) -> name1.append(", ").append(name2))
                                            .orElseGet(StringBuilder::new)
                                            .toString() + "]");
                                }
                            } else {
                                Logger.getLogger(ModelsDomains.class.getName()).log(Level.WARNING, "Property '" + propertyName + "' is not suitable for scalar property name generation in model entity '" + sourceEntity.modelName + "'");
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
                        Logger.getLogger(ModelsDomains.class.getName()).log(Level.WARNING, "Target model entity '" + reference.destination + "' is not found in reference '" + reference.source + "." + reference.property + "'");
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
                        entityPath = entities.getEntitiesRoot().resolve(entityRefName).normalize();
                    }
                    Path entityRelativePath = entities.getEntitiesRoot().relativize(entityPath);
                    String entityRef = entityRelativePath.toString().replace('\\', '/');
                    SqlEntity entity = entities.loadEntity(entityRef);
                    if (entity.isCommand()) {
                        throw new IllegalStateException("Model entity can't be based on a DML query ('" + modelEntityName + "' in model '" + modelPath + "').");
                    }
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
                                            Utils.javaType(fieldsByProperty.get(fieldToProperty(referenceJson.getKey()))),
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
                    EntityField keyField;
                    if (keyNode != null && keyNode.isTextual() && !keyNode.asText().isEmpty()) {
                        // This should be a field name, but we want to be able to use both 'pet_id' or 'petId' names
                        String keyPropertyName = fieldToProperty(keyNode.asText());
                        keyField = fieldsByProperty.get(keyPropertyName);
                    } else {
                        List<EntityField> pks = fieldsByProperty.values().stream()
                                .filter(Field::isPk)
                                .collect(Collectors.toList());
                        if (pks.size() == 1) {
                            keyField = pks.get(0);
                        } else if (pks.isEmpty()) {
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
                                keyField.getName(),
                                Utils.javaType(keyField),
                                Utils.javaType(keyField, true),
                                fieldsByProperty,
                                references);
                    } else {
                        throw new IllegalStateException("Model entity '" + modelEntityName + "' in model '" + modelPath + "' doesn't contain key field");
                    }
                })
                .collect(Collectors.toMap(modelEntity -> modelEntity.modelName, Function.identity()));
    }

    /**
     * Transforms a name like {@code customer_id} into a name like {@code customerId}
     * It is idempotent. So if model's references are declared under names {@code customer_id} or {@code customerId} in *.model.json, - they are equivalent.
     * Warning! If such declarations are present in both forms, only one will survive.
     *
     * @param aFieldName A name like {@code customer_id}.
     * @return A name like {@code customerId}.
     */
    private static String fieldToProperty(String aFieldName) {
        String accessor = Utils.toPascalCase(aFieldName);
        return accessor.substring(0, 1).toLowerCase() + accessor.substring(1);
    }

}
