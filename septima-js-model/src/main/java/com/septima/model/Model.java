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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Model {

    public class Entity<K, D> {
        private final SqlQuery query;
        private final String keyFieldName;
        private final PropertyChangeListener changesReflector;
        private final Map<K, D> byKey = new HashMap<>();
        private final Function<D, K> keyOf;
        private final BiFunction<Map<String, Object>, PropertyChangeListener, D> forwardMapper;
        private final Function<D, Map<String, Object>> reverseMapper;
        private final Consumer<D> handler;

        public Entity(String anEntityName, String aKeyFieldName, Function<D, K> aKeyOf,
                      BiFunction<Map<String, Object>, PropertyChangeListener, D> aForwardMapper,
                      Function<D, Map<String, Object>> aReverseMapper) {
            this(anEntityName, aKeyFieldName, aKeyOf, aForwardMapper, aReverseMapper, (instance) -> {});
            //entities.add(this);
        }

        public Entity(String anEntityName, String aKeyFieldName, Function<D, K> aKeyOf,
                      BiFunction<Map<String, Object>, PropertyChangeListener, D> aForwardMapper,
                      Function<D, Map<String, Object>> aReverseMapper,
                      Consumer<D> aHandler) {
            query = sqlEntities.loadEntity(anEntityName).toQuery();
            changesReflector = listener(query, aKeyFieldName, aKeyOf);
            keyFieldName = aKeyFieldName;
            keyOf = aKeyOf;
            forwardMapper = aForwardMapper;
            reverseMapper = aReverseMapper;
            handler = aHandler;
        }

        public String getName() {
            return query.getEntityName();
        }

        public Map<K, D> getByKey() {
            return byKey;
        }

        public CompletableFuture<Map<K, D>> query(Map<String, Object> parameters) {
            return query.requestData(parameters)
                    .thenApply(data -> toDomain(query.getEntityName(),
                            keyFieldName,
                            keyOf,
                            datum -> forwardMapper.apply(datum, changesReflector),
                            reverseMapper,
                            data,
                            byKey
                    ))
                    .thenApply(data -> {
                                data.values().forEach(handler);
                                return data;
                            }
                    );
        }
    }

    private final SqlEntities sqlEntities;
    //private final List<Entity> entities = new ArrayList<>();

    public Model(SqlEntities aEntities) {
        sqlEntities = aEntities;
    }

    private <K, D> Map<K, D> toDomain(
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

    private final List<EntityAction> changes = new ArrayList<>();

    private <D, K> PropertyChangeListener listener(SqlQuery query, String pkName, Function<D, K> keyMapper) {
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
        List<CompletableFuture<Integer>> futures = sqlEntities.bindChanges(changes).entrySet().stream()
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
