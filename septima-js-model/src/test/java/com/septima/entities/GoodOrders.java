/*
 * This source file is generated automatically.
 * Please, don't edit it manually.
 */
package com.septima.entities;

import com.septima.entities.customers.CustomersRow;
import com.septima.entities.goods.GoodsRow;
import com.septima.entities.orders.OrdersRow;
import com.septima.model.Model;

import java.util.*;

public class GoodOrders extends Model {

    public class Good extends GoodsRow {

        @Override
        public void setId(long aValue) {
            long old = getId();
            if (goods.getByKey().containsKey(old)){
                goods.getByKey().remove(old);
                super.setId(aValue);
                goods.getByKey().put(aValue, this);
            } else {
                super.setId(aValue);
            }
        }

        public Collection<Order> getOrders() {
            return ordersByGoodId.computeIfAbsent(getId(), key -> new HashSet<>());
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
            instance -> map(
                    entry("id", instance.getId()),
                    entry("name", instance.getName())
            ),
            instance -> {
            },
            instance -> {
            }
    );

    public class Order extends OrdersRow {

        @Override
        public void setId(long aValue) {
            long old = getId();
            if (orders.getByKey().containsKey(old)){
                orders.getByKey().remove(old);
                super.setId(aValue);
                orders.getByKey().put(aValue, this);
            } else {
                super.setId(aValue);
            }
        }

        public Customer getSeller() {
            if (getSellerId() != null) {
                return Optional.ofNullable(customers.getByKey().get(getSellerId()))
                        .orElseThrow(() -> new IllegalStateException("Unresolved reference '" + customers.getName() + " (" + getSellerId() + ")' in entity '" + orders.getName() + " (" + getId() + ")'"));
            } else {
                return null;
            }
        }

        public void setSeller(Customer aCustomer) {
            setSellerId(aCustomer != null ? aCustomer.getId() : null);
        }

        @Override
        public void setSellerId(Long aValue){
            Long old = getSellerId();
            if (old != null ? !old.equals(aValue) : aValue != null){
                if (old != null){
                    fromGroups(this, ordersBySellerId, old);
                }
                super.setSellerId(aValue);
                if (aValue != null){
                    toGroups(this, ordersBySellerId, aValue);
                }
            }
        }

        public Customer getCustomer() {
            return Optional.ofNullable(customers.getByKey().get(getCustomerId()))
                    .orElseThrow(() -> new IllegalStateException("Unresolved reference '" + customers.getName() + " (" + getCustomerId() + ")' in entity '" + orders.getName() + " (" + getId() + ")'"));
        }

        public void setCustomer(Customer aCustomer) {
            setCustomerId(aCustomer.getId());
        }

        @Override
        public void setCustomerId(long aValue){
            long old = getCustomerId();
            if(old != aValue){
                fromGroups(this, ordersByCustomerId, old);
                super.setCustomerId(aValue);
                toGroups(this, ordersByCustomerId, aValue);
            }
        }

        public Good getGood() {
            return Optional.ofNullable(goods.getByKey().get(getGoodId()))
                    .orElseThrow(() -> new IllegalStateException("Unresolved reference '" + goods.getName() + " (" + getGoodId() + ")' in entity '" + orders.getName() + " (" + getId() + ")'"));
        }

        public void setGood(Good aGood) {
            setGoodId(aGood.getId());
        }

        @Override
        public void setGoodId(long aValue){
            long old = getGoodId();
            if(old != aValue){
                fromGroups(this, ordersByGoodId, old);
                super.setGoodId(aValue);
                toGroups(this, ordersByGoodId, aValue);
            }
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
            instance -> map(
                    entry("id", instance.getId()),
                    entry("customer_id", instance.getCustomerId()),
                    entry("seller_id", instance.getSellerId()),
                    entry("good_id", instance.getGoodId()),
                    entry("comment", instance.getComment()),
                    entry("moment", instance.getMoment()),
                    entry("paid", instance.getPaid()),
                    entry("summ", instance.getSumm()),
                    entry("destination", instance.getDestination()),
                    entry("bad_data", instance.getBadData())
            ),
            instance -> {
                toGroups(instance, ordersBySellerId, instance.getSellerId());
                toGroups(instance, ordersByCustomerId, instance.getCustomerId());
                toGroups(instance, ordersByGoodId, instance.getGoodId());
            },
            instance -> {
                fromGroups(instance, ordersBySellerId, instance.getSellerId());
                fromGroups(instance, ordersByCustomerId, instance.getCustomerId());
                fromGroups(instance, ordersByGoodId, instance.getGoodId());
            }
    );

    public class Customer extends CustomersRow {

        @Override
        public void setId(long aValue) {
            long old = getId();
            if (customers.getByKey().containsKey(old)){
                customers.getByKey().remove(old);
                super.setId(aValue);
                customers.getByKey().put(aValue, this);
            } else {
                super.setId(aValue);
            }
        }

        public Collection<Order> getOrders() {
            return ordersByCustomerId.computeIfAbsent(getId(), key -> new HashSet<>());
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
            instance -> map(
                    entry("id", instance.getId()),
                    entry("name", instance.getName())
            ),
            instance -> {
            },
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

    public Good newGood() {
        return new Good();
    }

    public Order newOrder() {
        return new Order();
    }

    public Customer newCustomer() {
        return new Customer();
    }

}
