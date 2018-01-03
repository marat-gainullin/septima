package com.septima.sample;

import com.septima.entities.SqlEntities;
import com.septima.entities.customers.CustomersRow;
import com.septima.entities.goods.GoodsRow;
import com.septima.entities.orders.OrdersRow;
import com.septima.model.Model;

import java.util.*;

public class OrdersModel extends Model {

    public class Customer extends CustomersRow {
        public Collection<Order> getOrders() {
            return ordersByCustomerKey.getOrDefault(getId(), List.of());
        }
    }

    private final Entity<Long, Customer> customers = new Entity<>(
            "entities/query",
            "id",
            Customer::getId,
            (datum, changesReflector) -> {
                Customer instance = new Customer();
                instance.setId((long) datum.get("id"));
                instance.setName((String) datum.get("name"));
                instance.addListener(changesReflector);
                return instance;
            },
            instance -> Map.ofEntries(
                    Map.entry("id", instance.getId()),
                    Map.entry("name", instance.getName())
            )
    );

    public class Good extends GoodsRow {
        public Collection<Order> getOrders() {
            return ordersByGoodKey.getOrDefault(getId(), List.of());
        }
    }

    private Entity<Long, Good> goods = new Entity<>(
            "entities/goods",
            "id",
            Good::getId,
            (datum, changesReflector) -> {
                Good instance = new Good();
                instance.setId((long) datum.get("id"));
                instance.setName((String) datum.get("name"));
                instance.addListener(changesReflector);
                return instance;
            },
            instance -> Map.ofEntries(
                    Map.entry("id", instance.getId()),
                    Map.entry("name", instance.getName())
            )
    );

    public class Order extends OrdersRow {

        public Customer getCustomer() {
            // For required fields
            return Optional.ofNullable(customers.getByKey().get(getCustomerId()))
                    .orElseThrow(() -> new IllegalStateException("Unresolved reference '" + customers.getName() + "(" + getCustomerId() + ")' in entity '" + orders.getName() + "(" + getId() + ")'"));
            // For Nullable fields
            /*
            if (getCustomerId() != null) {
                return Optional.ofNullable(customers.getByKey().get(getCustomerId()))
                        .orElseThrow(() -> new IllegalStateException("Unresolved reference '" + customers.getName() + "(" + getCustomerId() + ")' in entity '" + orders.getName() + "(" + getId() + ")'"));
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
            return Optional.ofNullable(goods.getByKey().get(getGoodId()))
                    .orElseThrow(() -> new IllegalStateException("Unresolved reference '" + goods.getName() + "(" + getGoodId() + ")' in entity '" + orders.getName() + "(" + getId() + ")'"));
            // For nullable fields
            /*
            if (getGoodId() != null) {
                return Optional.ofNullable(goods.getByKey().get(getGoodId()))
                        .orElseThrow(() -> new IllegalStateException("Unresolved reference '" + goods.getName() + "(" + getGoodId() + ")' in entity '" + orders.getName() + "(" + getId() + ")'"));
            } else {
                return null;
            }
            */
        }

        public void setGood(Good aGood) {
            // For required fields
            Good old = getGood();
            old.getOrders().remove(this);
            setGoodId(aGood.getId());
            aGood.getOrders().add(this);
            // For nullable fields
            /*
            Good old = getGood();
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

    private Map<Long, Collection<Order>> ordersByCustomerKey = new HashMap<>();
    private Map<Long, Collection<Order>> ordersByGoodKey = new HashMap<>();

    private final Entity<Long, Order> orders = new Entity<>(
            "entities/orders",
            "id",
            Order::getId,
            (datum, ordersChangesReflector) -> {
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
            instance -> {
                toGroups(instance, ordersByCustomerKey, instance.getCustomerId());
                toGroups(instance, ordersByGoodKey, instance.getGoodId());
            }
    );

    public OrdersModel(SqlEntities aEntities) {
        super(aEntities);
    }

    public Entity<Long, Good> getGoods() {
        return goods;
    }

    public Entity<Long, Customer> getCustomers() {
        return customers;
    }

    public Entity<Long, Order> getOrders() {
        return orders;
    }
}
