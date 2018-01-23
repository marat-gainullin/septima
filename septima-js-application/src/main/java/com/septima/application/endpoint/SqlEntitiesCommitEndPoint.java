package com.septima.application.endpoint;

import com.septima.GenericType;
import com.septima.application.exceptions.EndPointException;
import com.septima.application.exceptions.NoCollectionException;
import com.septima.application.exceptions.NotPublicException;
import com.septima.application.exceptions.WritesNotAllowedException;
import com.septima.changes.*;
import com.septima.entities.SqlEntity;

import javax.servlet.http.HttpServletRequest;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlEntitiesCommitEndPoint extends SqlEntitiesEndPoint {

    private List<EntityAction> toActions(List<Map<String, Object>> aJsonActions) {
        return aJsonActions.stream()
                .map(entry -> {
                    if (entry.containsKey("kind")) {
                        if (entry.containsKey("entity")) {
                            String entityName = (String) entry.get("entity");
                            try {
                                SqlEntity entity = entities.loadEntity(entityName);
                                String kind = (String) entry.get("kind");
                                if ("command".equalsIgnoreCase(kind)) {
                                    Map<String, Object> parameters = (Map<String, Object>) entry.getOrDefault("parameters", Map.of());
                                    reviveDates(parameters, parametersTypes(entity));
                                    return new EntityCommand(entityName, parameters);
                                } else if ("insert".equalsIgnoreCase(kind)) {
                                    Map<String, Object> data = (Map<String, Object>) entry.getOrDefault("data", Map.of());
                                    reviveDates(data, fieldsTypes(entity));
                                    return new InstanceAdd(entityName, data);
                                } else if ("update".equalsIgnoreCase(kind)) {
                                    Map<String, Object> keys = (Map<String, Object>) entry.getOrDefault("keys", Map.of());
                                    Map<String, Object> data = (Map<String, Object>) entry.getOrDefault("data", Map.of());
                                    Map<String, GenericType> types = fieldsTypes(entity);
                                    reviveDates(keys, types);
                                    reviveDates(data, types);
                                    return new InstanceChange(entityName, keys, data);
                                } else if ("delete".equalsIgnoreCase(kind)) {
                                    Map<String, Object> keys = (Map<String, Object>) entry.getOrDefault("keys", Map.of());
                                    reviveDates(keys, fieldsTypes(entity));
                                    return new InstanceRemove(entityName, keys);
                                } else {
                                    throw new EndPointException("Unknown commit log entry kind: '" + kind+"'");
                                }
                            } catch (UncheckedIOException ex) {
                                if (ex.getCause() instanceof FileNotFoundException) {
                                    throw new NoCollectionException(entityName);
                                } else {
                                    throw ex;
                                }
                            }
                        } else {
                            throw new EndPointException("Commit log entry have to contain 'entity' property");
                        }
                    } else {
                        throw new EndPointException("Commit log entry have to contain 'kind' property");
                    }
                }).collect(Collectors.toList());
    }

    private List<EntityAction> checkAccess(List<EntityAction> actions, HttpServletRequest request) {
        actions.forEach(entry -> {
            SqlEntity entity = entities.loadEntity(entry.getEntityName());
            if (!entity.isPublicAccess()) {
                throw new NotPublicException(entry.getEntityName());
            } else if (!entity.getWriteRoles().isEmpty() && entity.getWriteRoles().stream().noneMatch(request::isUserInRole)) {
                throw new WritesNotAllowedException(entry.getEntityName(), entity.getWriteRoles());
            }
        });
        return actions;
    }

    private CompletableFuture<Integer> apply(List<EntityAction> actions) {
        List<CompletableFuture<Integer>> futures = entities.bindChanges(actions).entrySet().stream()
                .map(entry -> entry.getKey().commit(entry.getValue()))
                .collect(Collectors.toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{}))
                .thenApply(v -> futures.stream()
                        .map(f -> f.getNow(0))
                        .reduce(Integer::sum).orElse(0)
                );
    }

    @Override
    public void post(Answer answer) {
        answer.onJsonArray()
                .thenApply(entries -> toActions(entries))
                .thenApply(actions -> checkAccess(actions, answer.getRequest()))
                .thenApply(actions -> apply(actions))
                .thenCompose(Function.identity())
                .thenAccept(affected -> answer.withJsonObject(Map.of("affected", affected)))
                .exceptionally(answer::exceptionally);
    }
}
