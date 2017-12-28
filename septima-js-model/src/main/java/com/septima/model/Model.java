package com.septima.model;

import com.septima.changes.EntityAction;
import com.septima.changes.EntityAdd;
import com.septima.changes.EntityChange;
import com.septima.changes.EntityRemove;
import com.septima.entities.SqlEntities;
import com.septima.queries.SqlQuery;
import javafx.collections.FXCollections;
import javafx.collections.MapChangeListener;
import javafx.collections.ObservableMap;

import java.beans.PropertyChangeListener;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Model {

    protected final SqlEntities entities;

    public Model(SqlEntities aEnities){
        entities = aEnities;
    }

    protected <K, D> Map<K, D> toDomainMap(
            String entityName,
            String keyName,
            Function<D, K> keyMapper,
            Function<Map<String, Object>, D> forwardMapper,
            Function<D, Map<String, Object>> reverseMapper,
            Collection<Map<String, Object>> data,
            Map<K, D> fetched
    ) {
        ObservableMap<K, D> observable = FXCollections.observableMap(
                data.stream()
                        .map(datum -> {
                            D domainValue = forwardMapper.apply(datum);
                            K key = keyMapper.apply(domainValue);
                            fetched.putIfAbsent(key, domainValue);
                            return fetched.get(key);
                        })
                        .collect(Collectors.toMap(keyMapper, Function.identity())));
        observable.addListener((MapChangeListener.Change<? extends K, ? extends D> change) -> {
                    if (change.wasAdded()) {
                        changes.add(new EntityAdd(entityName, reverseMapper.apply(change.getValueAdded())));
                    }
                    if (change.wasRemoved()) {
                        changes.add(new EntityRemove(entityName, Map.of(keyName, keyMapper.apply(change.getValueRemoved()))));
                    }
                }
        );
        return observable;
    }

    protected static <K, D> void toGroups(D instance, Map<K, Collection<D>> groups, K key) {
        groups.computeIfAbsent(key, k -> new HashSet<>()).add(instance);
    }

    protected final List<EntityAction> changes = new ArrayList<>();

    protected <D> PropertyChangeListener listener(SqlQuery query, String pkName, Function<D, Object> keyMapper) {
        return e ->
                changes.add(new EntityChange(
                        query.getEntityName(),
                        Map.of(pkName, pkName.equals(e.getPropertyName()) ? e.getOldValue() : keyMapper.apply((D) e.getSource())),
                        Map.of(e.getPropertyName(), e.getNewValue())));
    }

    public void dropChanges() {
        changes.clear();
    }

    public CompletableFuture<Integer> save() {
        List<CompletableFuture<Integer>> futures = entities.bindChanges(changes).entrySet().stream()
                .map(entry -> entry.getKey().commit(entry.getValue()))
                .collect(Collectors.toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{}))
                .thenApply(v -> {
                    changes.clear();
                    return futures.stream()
                            .map(f -> f.getNow(0))
                            .reduce(Integer::sum).orElse(0);
                });
    }
}
