package com.septima.generator;

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
import static org.junit.Assert.assertTrue;

public class EntitiesRowsTest {

    private static String rn2n(String withRn) {
        return withRn.replace("\r\n", "\n").replace("\n\r", "\n").replace("\r", "\n");
    }

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    @Test
    public void generateRows() throws IOException {
        Path testAppPath = new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath().resolve("rows");
        SqlEntities entities = new SqlEntities(
                testAppPath,
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        Path destination = new File(System.getProperty("generated.path")).toPath().resolve("rows");
        Path ethalons = new File(System.getProperty("ethalons.path")).toPath().resolve("rows");
        EntitiesRows generator = EntitiesRows.fromResources(entities, destination);
        assertTrue(generator.deepToJavaSources(testAppPath) > 0);
        String customersEntityPathName = "com/septima/entities/customers/CustomersRow.java";
        String goodsEntityPathName = "com/septima/entities/goods/GoodsRow.java";
        String ordersEntityPathName = "com/septima/entities/orders/OrdersRow.java";
        String ethalonCustomers = new String(Files.readAllBytes(ethalons.resolve(customersEntityPathName + ".ethalon")), StandardCharsets.UTF_8);
        String generatedCustomers = new String(Files.readAllBytes(destination.resolve(customersEntityPathName)), StandardCharsets.UTF_8);
        assertEquals(rn2n(ethalonCustomers), rn2n(generatedCustomers));
        String ethalonGoods = new String(Files.readAllBytes(ethalons.resolve(goodsEntityPathName + ".ethalon")), StandardCharsets.UTF_8);
        String generatedGoods = new String(Files.readAllBytes(destination.resolve(goodsEntityPathName)), StandardCharsets.UTF_8);
        assertEquals(rn2n(ethalonGoods), rn2n(generatedGoods));
        String ethalonOrders = new String(Files.readAllBytes(ethalons.resolve(ordersEntityPathName + ".ethalon")), StandardCharsets.UTF_8);
        String generatedOrders = new String(Files.readAllBytes(destination.resolve(ordersEntityPathName)), StandardCharsets.UTF_8);
        assertEquals(rn2n(ethalonOrders), rn2n(generatedOrders));
    }
}
