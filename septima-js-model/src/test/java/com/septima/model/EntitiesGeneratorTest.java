package com.septima.model;

import com.septima.TestDataSource;
import com.septima.entities.SqlEntities;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.NamingException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EntitiesGeneratorTest {

    private static String rn2n(String withRn) {
        return withRn.replace("\r\n", "\n").replace("\n\r", "\n").replace("\r", "\n");
    }

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    @Test
    public void generate() throws IOException {
        SqlEntities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        Path destination = new File(System.getProperty("generated.path")).toPath();
        Path ethalons = new File(System.getProperty("ethalons.path")).toPath();
        EntitiesGenerator generator = new EntitiesGenerator(entities, destination);
        generator.generateRows();
        String customersEntityPathName = "com/septima/entities/customers/CustomersRow.java";
        String goodsEntityPathName = "com/septima/entities/goods/GoodsRow.java";
        String ordersEntityPathName = "com/septima/entities/orders/OrdersRow.java";
        String ethalonCustomers = new String(Files.readAllBytes(ethalons.resolve(customersEntityPathName)), StandardCharsets.UTF_8);
        String generatedCustomers = new String(Files.readAllBytes(destination.resolve(customersEntityPathName)), StandardCharsets.UTF_8);
        assertEquals(rn2n(ethalonCustomers), rn2n(generatedCustomers));
        String ethalonGoods = new String(Files.readAllBytes(ethalons.resolve(goodsEntityPathName)), StandardCharsets.UTF_8);
        String generatedGoods = new String(Files.readAllBytes(destination.resolve(goodsEntityPathName)), StandardCharsets.UTF_8);
        assertEquals(rn2n(ethalonGoods), rn2n(generatedGoods));
        String ethalonOrders = new String(Files.readAllBytes(ethalons.resolve(ordersEntityPathName)), StandardCharsets.UTF_8);
        String generatedOrders = new String(Files.readAllBytes(destination.resolve(ordersEntityPathName)), StandardCharsets.UTF_8);
        assertEquals(rn2n(ethalonOrders), rn2n(generatedOrders));
        assertFalse(destination.resolve("entities/bad").toFile().exists());
    }
}
