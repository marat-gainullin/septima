import com.septima.queries.SqlQuery;

import java.beans.PropertyChangeListener;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

class OrdersModel extends Model {

    class Customer extends CustomerRow {
        public Collection<Order> getOrders() {
            return ordersByCustomerKey.getOrDefault(getId(), List.of());
        }
    }

    class Good extends GoodRow {
        public Collection<Order> getOrders() {
            return ordersByGoodKey.getOrDefault(getId(), List.of());
        }
    }

    class Order extends OrderRow {

        public Customer getCustomer() {
            if (getCustomerId() != null) {
                if (customersByKey.containsKey(getCustomerId())) {
                    return customersByKey.get(getCustomerId());
                } else {
                    throw new IllegalStateException("Unresolved reference '" + "Customer(" + getCustomerId() + ")' in entity '" + ORDERS.getEntityName() + "(" + getId() + ")'");
                }
            } else {
                return null;
            }
        }

        public void setCustomer(Customer aCustomer) {
            /* // For required fields
             * if(aCustomer != null){
             *   setCustomerId(aCustomer.getId());
             * }
             */
            setCustomerId(aCustomer != null ? aCustomer.getId() : null);
        }

        public Good getGood() {
            if (getGoodId() != null) {
                if (goodsByKey.containsKey(getGoodId())) {
                    return goodsByKey.get(getGoodId());
                } else {
                    throw new IllegalStateException("Unresolved reference '" + "Good(" + getGoodId() + ")' in entity '" + ORDERS.getEntityName() + "(" + getId() + ")'");
                }
            } else {
                return null;
            }
        }

        public void setGood(Good aGood) {
            /* // For required fields
             * if(aGood != null){
             *   setGoodId(aGood.getId());
             * }
             */
            setGoodId(aGood != null ? aGood.getId() : null);
        }
    }

    private static final SqlQuery CUSTOMERS = ENTITIES.loadEntity("entities/customersByKey").toQuery();
    private final PropertyChangeListener customerChangesReflector = listener(CUSTOMERS, "id", Customer::getId);
    private final Map<Long, Customer> customersByKey = new HashMap<>();

    public CompletableFuture<Map<Long, Customer>> requestCustomers(Map<String, Object> parameters) {
        return CUSTOMERS.requestData(parameters)
                .thenApply(data -> toDomainMap(CUSTOMERS.getEntityName(),
                        "id",
                        Customer::getId,
                        datum -> {
                            Customer customer = new Customer();
                            customer.setId((long) datum.get("id"));
                            customer.setName((String) datum.get("name"));
                            customer.addListener(customerChangesReflector);
                            return customer;
                        },
                        customer -> Map.ofEntries(
                                Map.entry("id", customer.getId()),
                                Map.entry("name", customer.getName())
                        ),
                        data,
                        customersByKey
                ));
    }


    private static final SqlQuery GOODS = ENTITIES.loadEntity("entities/goodsByKey").toQuery();
    private final PropertyChangeListener goodChangesReflector = listener(GOODS, "id", Good::getId);
    private final Map<Long, Good> goodsByKey = new HashMap<>();

    public CompletableFuture<Map<Long, Good>> requestGoods(Map<String, Object> parameters) {
        return GOODS.requestData(parameters)
                .thenApply(data -> toDomainMap(CUSTOMERS.getEntityName(),
                        "id",
                        Good::getId,
                        datum -> {
                            Good good = new Good();
                            good.setId((long) datum.get("id"));
                            good.setName((String) datum.get("name"));
                            good.addListener(goodChangesReflector);
                            return good;
                        },
                        good -> Map.ofEntries(
                                Map.entry("id", good.getId()),
                                Map.entry("name", good.getName())
                        ),
                        data,
                        goodsByKey
                ));
    }


    private static final SqlQuery ORDERS = ENTITIES.loadEntity("entities/ordersByKey").toQuery();
    private final PropertyChangeListener orderChangesReflector = listener(ORDERS, "id", Order::getId);
    private final Map<Long, Order> ordersByKey = new HashMap<>();
    private final Map<Long, Collection<Order>> ordersByCustomerKey = new HashMap<>();
    private final Map<Long, Collection<Order>> ordersByGoodKey = new HashMap<>();

    public CompletableFuture<Map<Long, Order>> requestOrders(Map<String, Object> parameters) {
        return ORDERS.requestData(parameters)
                .thenApply(data -> toDomainMap(CUSTOMERS.getEntityName(),
                        "id",
                        Order::getId,
                        datum -> {
                            Order order = new Order();
                            order.setId((long) datum.get("id"));
                            order.setComment((String) datum.get("comment"));
                            order.setCustomerId((Long) datum.get("customer_id"));
                            order.setGoodId((Long) datum.get("good_id"));
                            order.addListener(orderChangesReflector);
                            return order;
                        },
                        order -> Map.ofEntries(
                                Map.entry("id", order.getId()),
                                Map.entry("comment", order.getComment()),
                                Map.entry("customer_id", order.getCustomerId()),
                                Map.entry("good_id", order.getGoodId())
                        ),
                        data,
                        ordersByKey
                ));
    }
}
