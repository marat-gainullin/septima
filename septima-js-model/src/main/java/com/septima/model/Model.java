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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Model {

    public class Entity<K, D extends Observable> {
        private final SqlQuery query;
        private final String keyFieldName;
        private final PropertyChangeListener changesReflector;
        private final Map<K, D> byKey = new HashMap<>();
        private final Function<D, K> keyOf;
        private final Function<Map<String, Object>, D> forwardMapper;
        private final Function<D, Map<String, Object>> reverseMapper;
        private final Consumer<D> handler;

        public Entity(String anEntityName, String aKeyFieldName, Function<D, K> aKeyOf,
                      Function<Map<String, Object>, D> aForwardMapper,
                      Function<D, Map<String, Object>> aReverseMapper) {
            this(anEntityName, aKeyFieldName, aKeyOf, aForwardMapper, aReverseMapper, (instance) -> {
            });
        }

        public Entity(String anEntityName, String aKeyFieldName, Function<D, K> aKeyOf,
                      Function<Map<String, Object>, D> aForwardMapper,
                      Function<D, Map<String, Object>> aReverseMapper,
                      Consumer<D> aHandler) {
            query = sqlEntities.loadEntity(anEntityName).toQuery();
            changesReflector = listener(query, aKeyFieldName, aKeyOf);
            keyFieldName = aKeyFieldName;
            keyOf = aKeyOf;
            forwardMapper = aForwardMapper;
            reverseMapper = aReverseMapper;
            handler = aHandler;
            //entities.add(this);
        }

        public String getName() {
            return query.getEntityName();
        }

        public Map<K, D> getByKey() {
            return byKey;
        }

        public CompletableFuture<Map<K, D>> query(Map<String, Object> parameters) {
            return query.requestData(parameters)
                    .thenApply(this::toDomain);
        }

        /**
         * Transforms untyped data to map of domain entities.
         * It is used in {@link #query(Map)} method to process results of a query.
         * It also can be used separately with any data. For example, {@code data} can be received from
         * http request/response, or manual {@link SqlQuery} execution. Last case is useful when you want
         * to fetch some data only once with use of a {@code JOIN} and construct multiple domain entities from that data.
         * A classic ORM would do the same thing when it is asked to fetched linked entities with 'EAGER' policy.
         * In both cases you are able to use navigation properties as usual.
         *
         * @param data Collection of #Map instances with properties of domain objects in key-value form.
         * @return Map of domain objects, identified by values of a key property.
         */
        public Map<K, D> toDomain(Collection<Map<String, Object>> data) {
            Map<K, D> domainData = data.stream()
                            .map(datum -> {
                                D instance = forwardMapper.apply(datum);
                                K key = keyOf.apply(instance);
                                byKey.putIfAbsent(key, instance);
                                return byKey.get(key);
                            })
                            .collect(Collectors.toMap(keyOf, Function.identity()));
            domainData.values().forEach(handler);
            ObservableMap<K, D> observable = FXCollections.observableMap(domainData);
            observable.addListener((MapChangeListener.Change<? extends K, ? extends D> change) -> {
                        if (change.wasAdded()) {
                            D added = change.getValueAdded();
                            added.addListener(changesReflector);
                            changes.add(new EntityAdd(query.getEntityName(), reverseMapper.apply(added)));
                        }
                        if (change.wasRemoved()) {
                            D removed = change.getValueRemoved();
                            removed.removeListener(changesReflector);
                            changes.add(new EntityRemove(query.getEntityName(), Map.of(keyFieldName, keyOf.apply(removed))));
                        }
                    }
            );
            return observable;
        }
    }

    private final SqlEntities sqlEntities;
    //private final Collection<Entity> entities = new ArrayList<>();

    public Model(SqlEntities aEntities) {
        sqlEntities = aEntities;
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

    protected static <K, D> void toGroups(D instance, Map<K, Collection<D>> groups, K key) {
        groups.computeIfAbsent(key, k -> new HashSet<>()).add(instance);
    }

}
