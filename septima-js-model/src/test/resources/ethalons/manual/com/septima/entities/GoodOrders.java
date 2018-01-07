package com.septima.entities;

import com.septima.entities.SqlEntities;
import com.septima.entities.goods.GoodsRow;
import com.septima.entities.orders.OrdersRow;
import com.septima.entities.customers.CustomersRow;

import com.septima.model.Model;

import java.util.*;

public class GoodOrders extends Model {

    public class Good extends GoodsRow {

        public Collection<Order> getOrders() {
            return ordersByGoodId.getOrDefault(getId(), List.of());
        }

    }

    private final Entity<Long, Good> goods = new Entity<>(
            "com/septima/entities/goods/goods",
            "id",
            Good::getId,
            datum -> {
                Good instance = new Good();

                instance.setId((long) datum.get("id"));
                instance.setName((String) datum.get("name"));

                return instance;
            },
            instance -> Map.ofEntries(
                    Map.entry("id", instance.getId()),
                    Map.entry("name", instance.getName())
            ),
            instance -> {
            }
    );

    public class Order extends OrdersRow {

        public Seller getSeller() {
            if (getSellerId() != null) {
                return Optional.ofNullable(sellers.getByKey().get(getSellerId()))
                        .orElseThrow(() -> new IllegalStateException("Unresolved reference '" + sellers.getName() + "' (" + getSellerId() + ")' in entity '" + orders.getName() + "' (" + getId() + ")'"));
            } else {
                return null;
            }
        }

        public void setSeller(Seller aSeller) {
            Seller old = getSeller();
            if (old != null) {
                old.getOrders().remove(this);
            }
            setSellerId(aSeller != null ? aSeller.getId() : null);
            if (aSeller != null) {
                aSeller.getOrders().add(this);
            }
        }

        public Customer getCustomer() {
            return Optional.ofNullable(customers.getByKey().get(getCustomerId()))
                    .orElseThrow(() -> new IllegalStateException("Unresolved reference '" + customers.getName() + "' (" + getCustomerId() + ")' in entity '" + orders.getName() + "' (" + getId() + ")'"));
        }

        public void setCustomer(Customer aCustomer) {
            Customer old = getCustomer();
            old.getOrders().remove(this);
            setCustomerId(aCustomer.getId());
            aCustomer.getOrders().add(this);
        }

        public Good getGood() {
            return Optional.ofNullable(goods.getByKey().get(getGoodId()))
                    .orElseThrow(() -> new IllegalStateException("Unresolved reference '" + goods.getName() + "' (" + getGoodId() + ")' in entity '" + orders.getName() + "' (" + getId() + ")'"));
        }

        public void setGood(Good aGood) {
            Good old = getGood();
            old.getOrders().remove(this);
            setGoodId(aGood.getId());
            aGood.getOrders().add(this);
        }

    }

    private Map<Long, Collection<Order>> ordersBySellerId = new HashMap<>();
    private Map<Long, Collection<Order>> ordersByCustomerId = new HashMap<>();
    private Map<Long, Collection<Order>> ordersByGoodId = new HashMap<>();

    private final Entity<Long, Order> orders = new Entity<>(
            "com/septima/entities/orders/orders",
            "id",
            Order::getId,
            datum -> {
                Order instance = new Order();

                instance.setId((long) datum.get("id"));
                instance.setCustomerId((long) datum.get("customer_id"));
                instance.setSellerId((Long) datum.get("seller_id"));
                instance.setGoodId((long) datum.get("good_id"));
                instance.setComment((String) datum.get("comment"));
                instance.setMoment((Date) datum.get("moment"));
                instance.setPaid((Boolean) datum.get("paid"));
                instance.setSumm((Double) datum.get("summ"));
                instance.setDestination((String) datum.get("destination"));
                instance.setBadData((String) datum.get("bad_data"));

                return instance;
            },
            instance -> Map.ofEntries(
                    Map.entry("id", instance.getId()),
                    Map.entry("customer_id", instance.getCustomerId()),
                    Map.entry("seller_id", instance.getSellerId()),
                    Map.entry("good_id", instance.getGoodId()),
                    Map.entry("comment", instance.getComment()),
                    Map.entry("moment", instance.getMoment()),
                    Map.entry("paid", instance.getPaid()),
                    Map.entry("summ", instance.getSumm()),
                    Map.entry("destination", instance.getDestination()),
                    Map.entry("bad_data", instance.getBadData())
            ),
            instance -> {
                toGroups(instance, ordersBySellerId, instance.getSellerId());
                toGroups(instance, ordersByCustomerId, instance.getCustomerId());
                toGroups(instance, ordersByGoodId, instance.getGoodId());
            }
    );

    public class Customer extends CustomersRow {

        public Collection<Order> getOrders() {
            return ordersByCustomerId.getOrDefault(getId(), List.of());
        }

    }

    private final Entity<Long, Customer> customers = new Entity<>(
            "com/septima/entities/customers/customers",
            "id",
            Customer::getId,
            datum -> {
                Customer instance = new Customer();

                instance.setId((long) datum.get("id"));
                instance.setName((String) datum.get("name"));

                return instance;
            },
            instance -> Map.ofEntries(
                    Map.entry("id", instance.getId()),
                    Map.entry("name", instance.getName())
            ),
            instance -> {
            }
    );

    public class Seller extends CustomersRow {

        public Collection<Order> getOrders() {
            return ordersBySellerId.getOrDefault(getId(), List.of());
        }

    }

    private final Entity<Long, Seller> sellers = new Entity<>(
            "com/septima/entities/customers/customers",
            "id",
            Seller::getId,
            datum -> {
                Seller instance = new Seller();

                instance.setId((long) datum.get("id"));
                instance.setName((String) datum.get("name"));

                return instance;
            },
            instance -> Map.ofEntries(
                    Map.entry("id", instance.getId()),
                    Map.entry("name", instance.getName())
            ),
            instance -> {
            }
    );

    public GoodOrders(SqlEntities aEntities) {
        super(aEntities);
    }

    public Entity<Long, Good> getGoods() {
        return goods;
    }

    public Entity<Long, Order> getOrders() {
        return orders;
    }

    public Entity<Long, Customer> getCustomers() {
        return customers;
    }

    public Entity<Long, Seller> getSellers() {
        return sellers;
    }

}
