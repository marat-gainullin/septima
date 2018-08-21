package com.septima.generator;

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
import static org.junit.Assert.assertTrue;

public class ModelsDomainsTest {

    private static String rn2n(String withRn) {
        return withRn.replace("\r\n", "\n").replace("\n\r", "\n").replace("\r", "\n");
    }

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    private void generateModel(String testAppSuffix) throws IOException {
        Path testAppPath = new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath().resolve(testAppSuffix);
        SqlEntities sqlEntities = new SqlEntities(
                testAppPath,
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        Path destination = new File(System.getProperty("generated.path")).toPath().resolve(testAppSuffix);
        EntitiesRaws javaEntities = EntitiesRaws.fromResources(sqlEntities, destination);
        javaEntities.deepToJavaSources(testAppPath);
        ModelsDomains javaModelsDomains = ModelsDomains.fromResources(sqlEntities, testAppPath, destination);
        assertTrue(javaModelsDomains.deepToJavaSources(testAppPath) > 0);
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
    public void generateModelWithAmbiguousPrimaryKey() throws IOException {
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
