package com.septima.model;

import com.septima.TestDataSource;
import com.septima.entities.GoodOrders;
import com.septima.entities.GoodOrders.Customer;
import com.septima.entities.GoodOrders.Good;
import com.septima.entities.GoodOrders.Order;
import com.septima.entities.SqlEntities;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.NamingException;
import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class GoodOrdersTest {

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    @Test(expected = IllegalStateException.class)
    public void entityWithRequiredUnresolvedReference() {
        Path testAppPath = new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath().resolve("auto");
        SqlEntities entities = new SqlEntities(
                testAppPath,
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME),
                true
        );
        GoodOrders model = new GoodOrders(entities);
        Order order = model.new Order();
        order.getGood();
    }

    @Test
    public void entityWithNullableUnresolvedReference() {
        Path testAppPath = new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath().resolve("auto");
        SqlEntities entities = new SqlEntities(
                testAppPath,
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME),
                true
        );
        GoodOrders model = new GoodOrders(entities);
        Order order = model.new Order();
        assertNull(order.getSeller());
    }

    @Test
    public void crudAutoEntities() throws InterruptedException, ExecutionException {
        Path testAppPath = new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath().resolve("auto");
        SqlEntities entities = new SqlEntities(
                testAppPath,
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME),
                true
        );
        createOrders(entities);
        checkReferences(entities);
        updateOrdersGoods(entities);
        deleteOrders(entities);
    }

    @Test
    public void dropChanges() throws InterruptedException, ExecutionException {
        Path testAppPath = new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath().resolve("auto");
        SqlEntities entities = new SqlEntities(
                testAppPath,
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME),
                true
        );
        GoodOrders model = new GoodOrders(entities);
        Map<Long, Good> goods = model.getGoods().query(Map.of()).get();

        Good good1 = model.new Good();
        good1.setId(Id.next());
        good1.setName("Good 1");
        goods.put(good1.getId(), good1);

        model.dropChanges();
        int affected = model.save().get();
        assertEquals(0, affected);
    }

    private void checkReferences(SqlEntities entities) throws InterruptedException, ExecutionException {
        GoodOrders model = new GoodOrders(entities);
        Map<Long, Good> goods = model.getGoods().query(Map.of()).get();
        assertNotNull(goods);
        assertEquals(2, goods.size());
        Map<Long, Customer> customers = model.getCustomers().query(Map.of()).get();
        assertNotNull(customers);
        assertEquals(4, customers.size());

        Customer customerOldRef = customers.values().iterator().next();
        Map<Long, Customer> customersNewQuery = model.getCustomers().query(Map.of()).get();
        Customer customerNewRef = customersNewQuery.values().stream()
                .filter(customer -> customer.getId() == customerOldRef.getId())
                .findAny()
                .get();
        assertSame(customerOldRef, customerNewRef);

        Map<Long, Order> orders = model.getOrders().query(Map.of()).get();
        assertNotNull(orders);
        assertEquals(2, orders.size());
        orders.values().forEach(order -> {
            assertEquals(1, order.getCustomer().getOrders().size());
            assertSame(order, order.getCustomer().getOrders().iterator().next());
            assertEquals(1, order.getGood().getOrders().size());
            assertSame(order, order.getGood().getOrders().iterator().next());
        });
    }

    private void deleteOrders(SqlEntities entities) throws InterruptedException, ExecutionException {
        GoodOrders model = new GoodOrders(entities);
        Map<Long, Good> goods = model.getGoods().query(Map.of()).get();
        Map<Long, Customer> customers = model.getCustomers().query(Map.of()).get();
        Map<Long, Order> orders = model.getOrders().query(Map.of()).get();
        orders.clear();
        customers.clear();
        goods.clear();
        int affected = model.save().get();
        assertEquals(8, affected);
    }

    private void updateOrdersGoods(SqlEntities entities) throws InterruptedException, ExecutionException {
        GoodOrders model = new GoodOrders(entities);
        Map<Long, Good> goods = model.getGoods().query(Map.of()).get();
        Map<Long, Customer> customers = model.getCustomers().query(Map.of()).get();
        Map<Long, Order> orders = model.getOrders().query(Map.of()).get();
        Iterator<Order> ordersIt = orders.values().iterator();
        Order order1 = ordersIt.next();
        Order order2 = ordersIt.next();
        Good good1 = order1.getGood();
        order1.setGood(order2.getGood());
        order2.setGood(good1);
        int affected = model.save().get();
        assertEquals(2, affected);
        Good newGood = model.new Good();
        newGood.setId(good1.getId());
        newGood.setName(good1.getName() + " updated");
        goods.put(newGood.getId(), newGood);
        int affected1 = model.save().get();
        assertEquals(1, affected1);
    }

    private void createOrders(SqlEntities entities) throws InterruptedException, ExecutionException {
        AtomicInteger sequence = new AtomicInteger();

        GoodOrders model = new GoodOrders(entities);
        Map<Long, Good> goods = model.getGoods().query(Map.of()).get();
        assertNotNull(goods);
        assertEquals(0, goods.size());
        Map<Long, Customer> customers = model.getCustomers().query(Map.of()).get();
        assertNotNull(customers);
        assertEquals(0, customers.size());
        Map<Long, Order> orders = model.getOrders().query(Map.of()).get();
        assertNotNull(orders);
        assertEquals(0, orders.size());

        Good good1 = model.new Good();
        good1.setId(sequence.incrementAndGet());
        good1.setName("Good 1");
        goods.put(good1.getId(), good1);

        Good good2 = model.newGood();
        good2.setId(sequence.incrementAndGet());
        good2.setName("Good 2");
        goods.put(good2.getId(), good2);

        Customer customer1 = model.new Customer();
        customer1.setId(sequence.incrementAndGet());
        customer1.setName("Customer 1");
        customers.put(customer1.getId(), customer1);

        Customer seller1 = model.new Customer();
        seller1.setId(sequence.incrementAndGet());
        seller1.setName("Seller 1");
        customers.put(seller1.getId(), seller1);

        Customer customer2 = model.new Customer();
        customer2.setId(sequence.incrementAndGet());
        customer2.setName("Customer 2");
        customers.put(customer2.getId(), customer2);

        Customer seller2 = model.new Customer();
        seller2.setId(sequence.incrementAndGet());
        seller2.setName("Seller 2");
        customers.put(seller2.getId(), seller2);

        Order order1 = model.new Order();
        order1.setId(sequence.incrementAndGet());
        order1.setSeller(seller1);
        order1.setCustomer(customer1);
        order1.setGood(good1);
        orders.put(order1.getId(), order1);
        assertEquals(1, order1.getCustomer().getOrders().size());
        assertSame(order1, order1.getCustomer().getOrders().iterator().next());
        assertEquals(1, order1.getGood().getOrders().size());
        assertSame(order1, order1.getGood().getOrders().iterator().next());

        Order order2 = model.new Order();
        order2.setId(sequence.incrementAndGet());
        order2.setSeller(seller2);
        order2.setCustomer(customer2);
        order2.setGoodId(good2.getId());
        orders.put(order2.getId(), order2);
        assertEquals(1, order2.getCustomer().getOrders().size());
        assertSame(order2, order2.getCustomer().getOrders().iterator().next());
        assertEquals(1, order2.getGood().getOrders().size());
        assertSame(order2, order2.getGood().getOrders().iterator().next());

        int affected = model.save().get();
        assertEquals(8, affected);
    }
}
