package com.septima.application.endpoint;

import com.septima.GenericType;
import com.septima.application.exceptions.EndPointException;
import com.septima.application.exceptions.NoCollectionException;
import com.septima.application.exceptions.NoInstanceException;
import com.septima.metadata.EntityField;
import com.septima.metadata.Field;

import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlEntitiesSchemaEndPoint extends SqlEntitiesEndPoint {

    private Map<String, Object> fromField(EntityField aField) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("type", aField.getType() != null ? aField.getType() : GenericType.STRING);
        properties.put("nullable", aField.isNullable());
        properties.put("description", aField.getDescription());
        properties.put("pk", aField.isPk());
        return properties;
    }

    @Override
    public void get(Answer answer) {
        onCollectionRef(entityRef -> {
            if (entities.exists(entityRef)) {
                onPublic(publicEntity -> onReadsAllowed(entity -> {
                    if(entity.isCommand()){
                        throw new EndPointException("Entity '" + entity.getName() + "' is command entity. It can't be used as a collection");
                    }
                    answer.withJsonObject(
                            entity.getFields().values().stream()
                                    .sorted(Comparator.comparing(Field::getName))
                                    .collect(Collectors.toMap(EntityField::getName, this::fromField))
                    );
                }, answer, publicEntity), answer, entities.loadEntity(entityRef));
            } else {
                int lastSlashAt = entityRef.lastIndexOf('/');
                if (lastSlashAt > 0 && lastSlashAt < entityRef.length() - 1) {
                    String anotherEntityRef = entityRef.substring(0, lastSlashAt);
                    String fieldName = entityRef.substring(lastSlashAt + 1);
                    try {
                        onPublic(publicEntity -> onReadsAllowed(entity -> {
                            if(entity.isCommand()){
                                throw new EndPointException("Entity '" + entity.getName() + "' is command entity. It can't be used as a collection");
                            }
                            if (entity.getFields().containsKey(fieldName)) {
                                answer.withJsonObject(fromField(entity.getFields().get(fieldName)));
                            } else {
                                throw new NoInstanceException(anotherEntityRef, "field.name", fieldName);
                            }
                        }, answer, publicEntity), answer, entities.loadEntity(anotherEntityRef));
                    } catch (UncheckedIOException aex) {
                        if (aex.getCause() instanceof FileNotFoundException) {
                            throw new NoCollectionException(entityRef + " or " + anotherEntityRef);
                        } else {
                            throw aex;
                        }
                    }
                } else {
                    throw new NoCollectionException(entityRef);
                }
            }
        }, answer);
    }
}
