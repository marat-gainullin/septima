package com.septima.application.endpoint;

import com.septima.GenericType;
import com.septima.application.exceptions.NoCollectionException;
import com.septima.application.exceptions.NoInstanceException;
import com.septima.metadata.Parameter;

import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlEntitiesParametersEndPoint extends SqlEntitiesEndPoint {

    private Map<String, Object> fromParameter(Parameter aParameter) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("type", aParameter.getType() != null ? aParameter.getType() : GenericType.STRING);
        properties.put("description", aParameter.getDescription());
        properties.put("mode", aParameter.getMode());
        return properties;
    }

    @Override
    public void get(Answer answer) {
        onCollectionRef(entityRef -> {
            if (entities.exists(entityRef)) {
                onPublic(publicEntity -> onReadsAllowed(entity -> {
                    answer.withJsonObject(
                            entity.getParameters().values().stream()
                                    .sorted(Comparator.comparing(Parameter::getName))
                                    .collect(Collectors.toMap(Parameter::getName, this::fromParameter))
                    );
                }, answer, publicEntity), answer, entities.loadEntity(entityRef));
            } else {
                int lastSlashAt = entityRef.lastIndexOf('/');
                if (lastSlashAt > 0 && lastSlashAt < entityRef.length() - 1) {
                    String anotherEntityRef = entityRef.substring(0, lastSlashAt);
                    String fieldName = entityRef.substring(lastSlashAt + 1);
                    try {
                        onPublic(publicEntity -> onReadsAllowed(entity -> {
                            if (entity.getParameters().containsKey(fieldName)) {
                                answer.withJsonObject(fromParameter(entity.getParameters().get(fieldName)));
                            } else {
                                throw new NoInstanceException(anotherEntityRef, "parameter.name", fieldName);
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
