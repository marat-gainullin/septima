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

public class EntitiesRawsTest {

    private static String rn2n(String withRn) {
        return withRn.replace("\r\n", "\n").replace("\n\r", "\n").replace("\r", "\n");
    }

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    @Test
    public void generateRaws() throws IOException {
        Path testAppPath = new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath().resolve("rows");
        SqlEntities entities = new SqlEntities(
                testAppPath,
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME),
                true
        );
        Path destination = new File(System.getProperty("generated.path")).toPath().resolve("rows");
        Path ethalons = new File(System.getProperty("ethalons.path")).toPath().resolve("rows");
        EntitiesRaws generator = EntitiesRaws.fromResources(entities, destination);
        assertTrue(generator.deepToJavaSources(testAppPath) > 0);
        String customersEntityPathName = "com/septima/entities/customers/CustomersRaw.java";
        String goodsEntityPathName = "com/septima/entities/goods/GoodsRaw.java";
        String ordersEntityPathName = "com/septima/entities/orders/OrdersRaw.java";
        String ethalonCustomers = Files.readString(ethalons.resolve(customersEntityPathName + ".ethalon"), StandardCharsets.UTF_8);
        String generatedCustomers = Files.readString(destination.resolve(customersEntityPathName), StandardCharsets.UTF_8);
        assertEquals(rn2n(ethalonCustomers), rn2n(generatedCustomers));
        String ethalonGoods = Files.readString(ethalons.resolve(goodsEntityPathName + ".ethalon"), StandardCharsets.UTF_8);
        String generatedGoods = Files.readString(destination.resolve(goodsEntityPathName), StandardCharsets.UTF_8);
        assertEquals(rn2n(ethalonGoods), rn2n(generatedGoods));
        String ethalonOrders = Files.readString(ethalons.resolve(ordersEntityPathName + ".ethalon"), StandardCharsets.UTF_8);
        String generatedOrders = Files.readString(destination.resolve(ordersEntityPathName), StandardCharsets.UTF_8);
        assertEquals(rn2n(ethalonOrders), rn2n(generatedOrders));
    }
}
