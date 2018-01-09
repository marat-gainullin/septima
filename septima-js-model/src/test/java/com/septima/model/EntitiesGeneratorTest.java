package com.septima.model;

import com.septima.TestDataSource;
import com.septima.entities.SqlEntities;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.NamingException;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class EntitiesGeneratorTest {

    private static String rn2n(String withRn) {
        return withRn.replace("\r\n", "\n").replace("\n\r", "\n").replace("\r", "\n");
    }

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    @Test
    public void generateRows() throws IOException, URISyntaxException {
        Path testAppPath = new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath().resolve("rows");
        SqlEntities entities = new SqlEntities(
                testAppPath,
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        Path destination = new File(System.getProperty("generated.path")).toPath().resolve("rows");
        Path ethalons = new File(System.getProperty("ethalons.path")).toPath().resolve("rows");
        EntitiesGenerator generator = EntitiesGenerator.fromResources(entities, testAppPath, destination);
        generator.generateRows();
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

    private void generateModel(String testAppSuffix) throws IOException, URISyntaxException {
        Path testAppPath = new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath().resolve(testAppSuffix);
        SqlEntities entities = new SqlEntities(
                testAppPath,
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        Path destination = new File(System.getProperty("generated.path")).toPath().resolve(testAppSuffix);
        EntitiesGenerator generator = EntitiesGenerator.fromResources(entities, testAppPath, destination);
        generator.generateRows();
        generator.generateModels();
    }

    private void checkModel(String testAppSuffix, String modelRelativePathName) throws IOException {
        Path destination = new File(System.getProperty("generated.path")).toPath().resolve(testAppSuffix);
        Path ethalons = new File(System.getProperty("ethalons.path")).toPath().resolve(testAppSuffix);
        String ethalonGoodOrders = new String(Files.readAllBytes(ethalons.resolve(modelRelativePathName + ".ethalon")), StandardCharsets.UTF_8);
        String generatedGoodOrders = new String(Files.readAllBytes(destination.resolve(modelRelativePathName)), StandardCharsets.UTF_8);
        assertEquals(rn2n(ethalonGoodOrders), rn2n(generatedGoodOrders));
    }

    private void generateAndCheckModel(String testAppSuffix, String modelRelativePathName) throws IOException, URISyntaxException {
        generateModel(testAppSuffix);
        checkModel(testAppSuffix, modelRelativePathName);
    }

    @Test
    public void generateModelWithManualReferences() throws IOException, URISyntaxException {
        generateAndCheckModel("manual", "com/septima/entities/GoodOrders.java");
    }

    @Test
    public void generateModelWithAutoReferences() throws IOException, URISyntaxException {
        generateAndCheckModel("auto", "com/septima/entities/GoodOrders.java");
    }

    @Test(expected = IllegalStateException.class)
    public void generateModelWithAmbiguousPrimaryKey() throws IOException, URISyntaxException {
        generateModel("ambiguous/key");
    }

    @Test
    public void generateModelWithAmbiguousReferences() throws IOException, URISyntaxException {
        generateAndCheckModel("ambiguous/references", "GoodOrders.java");
    }

    @Test
    public void generateModelWithAutoUnaryReference() throws IOException, URISyntaxException {
        generateAndCheckModel("unary/auto", "Goods.java");
    }

    @Test
    public void generateModelWithManualUnaryReference() throws IOException, URISyntaxException {
        generateAndCheckModel("unary/manual", "Goods.java");
    }

    @Test
    public void generateModelWithBadScalarReference() throws IOException, URISyntaxException {
        generateAndCheckModel("bad/scalar", "GoodOrders.java");
    }

    @Test
    public void generateModelWithBadCollectionReference() throws IOException, URISyntaxException {
        generateAndCheckModel("bad/collection", "GoodOrders.java");
    }
}
