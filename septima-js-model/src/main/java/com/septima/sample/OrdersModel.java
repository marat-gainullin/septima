package com.septima.sample;

import com.septima.entities.SqlEntities;
import com.septima.model.Model;
import com.septima.queries.SqlQuery;
import com.septima.entities.customers.CustomersRow;
import com.septima.entities.goods.GoodsRow;
import com.septima.entities.orders.OrdersRow;

import java.beans.PropertyChangeListener;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class OrdersModel extends Model {

    public class Customer extends CustomersRow {
        public Collection<Order> getOrders() {
            return ordersByCustomerKey.getOrDefault(getId(), List.of());
        }
    }

    public class Good extends GoodsRow {
        public Collection<Order> getOrders() {
            return ordersByGoodKey.getOrDefault(getId(), List.of());
        }
    }

    public class Order extends OrdersRow {

        public Customer getCustomer() {
            // For required fields
            if (customersByKey.containsKey(getCustomerId())) {
                return customersByKey.get(getCustomerId());
            } else {
                throw new IllegalStateException("Unresolved reference '" + customers.getEntityName() + "(" + getCustomerId() + ")' in entity '" + orders.getEntityName() + "(" + getId() + ")'");
            }
            // For Nullable fields
            /*
            if (getCustomerId() != null) {
                if (customersByKey.containsKey(getCustomerId())) {
                    return customersByKey.get(getCustomerId());
                } else {
                    throw new IllegalStateException("Unresolved reference '" + customers.getEntityName() + "(" + getCustomerId() + ")' in entity '" + orders.getEntityName() + "(" + getId() + ")'");
                }
            } else {
                return null;
            }
            */
        }


        public void setCustomer(Customer aCustomer) {
            Customer old = getCustomer();
            if (old != aCustomer) {
                // For required fields
                old.getOrders().remove(this);
                setCustomerId(aCustomer.getId());
                aCustomer.getOrders().add(this);

                // For nullable fields
                /*
                if (old != null) {
                    old.getOrders().remove(this);
                }
                setCustomerId(aCustomer != null ? aCustomer.getId() : null);
                if (aCustomer != null) {
                    aCustomer.getOrders().add(this);
                }
                */
            }
        }

        public Good getGood() {
            // For required fields
            if (goodsByKey.containsKey(getGoodId())) {
                return goodsByKey.get(getGoodId());
            } else {
                throw new IllegalStateException("Unresolved reference '" + goods.getEntityName() + "(" + getGoodId() + ")' in entity '" + orders.getEntityName() + "(" + getId() + ")'");
            }
            // For nullable fields
            /*
            if (getGoodId() != null) {
                if (goodsByKey.containsKey(getGoodId())) {
                    return goodsByKey.get(getGoodId());
                } else {
                    throw new IllegalStateException("Unresolved reference '" + goods.getEntityName() + "(" + getGoodId() + ")' in entity '" + orders.getEntityName() + "(" + getId() + ")'");
                }
            } else {
                return null;
            }
            */
        }

        public void setGood(Good aGood) {
            Good old = getGood();
            // For required fields
            old.getOrders().remove(this);
            setGoodId(aGood.getId());
            aGood.getOrders().add(this);
            // For nullable fields
            /*
            if (old != null) {
                old.getOrders().remove(this);
            }
            setGoodId(aGood != null ? aGood.getId() : null);
            if (aGood != null) {
                aGood.getOrders().add(this);
            }
            */
        }

    }

    public OrdersModel(SqlEntities aEntities) {
        super(aEntities);
    }

    private final SqlQuery customers = entities.loadEntity("entities/customers").toQuery();
    private final PropertyChangeListener customersChangesReflector = listener(customers, "id", Customer::getId);
    private final Map<Long, Customer> customersByKey = new HashMap<>();

    public CompletableFuture<Map<Long, Customer>> requestCustomers(Map<String, Object> parameters) {
        return customers.requestData(parameters)
                .thenApply(data -> toDomainMap(customers.getEntityName(),
                        "id",
                        Customer::getId,
                        datum -> {
                            Customer instance = new Customer();
                            instance.setId((long) datum.get("id"));
                            instance.setName((String) datum.get("name"));
                            instance.addListener(customersChangesReflector);
                            return instance;
                        },
                        instance -> Map.ofEntries(
                                Map.entry("id", instance.getId()),
                                Map.entry("name", instance.getName())
                        ),
                        data,
                        customersByKey
                ));
    }


    private final SqlQuery goods = entities.loadEntity("entities/goods").toQuery();
    private final PropertyChangeListener goodsChangesReflector = listener(goods, "id", Good::getId);
    private final Map<Long, Good> goodsByKey = new HashMap<>();

    public CompletableFuture<Map<Long, Good>> requestGoods(Map<String, Object> parameters) {
        return goods.requestData(parameters)
                .thenApply(data -> toDomainMap(goods.getEntityName(),
                        "id",
                        Good::getId,
                        datum -> {
                            Good instance = new Good();
                            instance.setId((long) datum.get("id"));
                            instance.setName((String) datum.get("name"));
                            instance.addListener(goodsChangesReflector);
                            return instance;
                        },
                        instance -> Map.ofEntries(
                                Map.entry("id", instance.getId()),
                                Map.entry("name", instance.getName())
                        ),
                        data,
                        goodsByKey
                ));
    }


    private final SqlQuery orders = entities.loadEntity("entities/orders").toQuery();
    private final PropertyChangeListener ordersChangesReflector = listener(orders, "id", Order::getId);
    private final Map<Long, Order> ordersByKey = new HashMap<>();
    private final Map<Long, Collection<Order>> ordersByCustomerKey = new HashMap<>();
    private final Map<Long, Collection<Order>> ordersByGoodKey = new HashMap<>();

    public CompletableFuture<Map<Long, Order>> requestOrders(Map<String, Object> parameters) {
        return orders.requestData(parameters)
                .thenApply(data -> toDomainMap(orders.getEntityName(),
                        "id",
                        Order::getId,
                        datum -> {
                            Order instance = new Order();
                            instance.setId((long) datum.get("id"));
                            instance.setComment((String) datum.get("comment"));
                            instance.setCustomerId((Long) datum.get("customer_id"));
                            instance.setGoodId((Long) datum.get("good_id"));
                            instance.addListener(ordersChangesReflector);
                            return instance;
                        },
                        instance -> Map.ofEntries(
                                Map.entry("id", instance.getId()),
                                Map.entry("comment", instance.getComment()),
                                Map.entry("customer_id", instance.getCustomerId()),
                                Map.entry("good_id", instance.getGoodId())
                        ),
                        data,
                        ordersByKey
                ))
                .thenApply(data -> {
                    data.values().forEach(instance -> {
                        toGroups(instance, ordersByCustomerKey, instance.getCustomerId());
                        toGroups(instance, ordersByGoodKey, instance.getGoodId());
                    });
                    return data;
                });
    }
}
