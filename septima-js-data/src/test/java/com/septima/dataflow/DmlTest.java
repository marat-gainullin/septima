package com.septima.dataflow;

import com.septima.Parameter;
import com.septima.TestDataSource;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import com.septima.queries.SqlQuery;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.NamingException;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class DmlTest {

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    @Test
    public void crudOwnParameters() throws Exception {
        int assetId = 123456;
        String assetName = "Septima 'crudOwnParameters' test asset";
        double assetField7 = 0.6d;
        SqlEntities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity insertEntity = entities.loadEntity("dml/insert-asset");
        assertNotNull(insertEntity);
        assertTrue(insertEntity.isCommand());
        assertTrue(insertEntity.getFields().isEmpty());
        assertEquals(3, insertEntity.getParameters().size());
        insertEntity.getParameters().get("id").setValue(assetId);
        insertEntity.getParameters().get("name").setValue(assetName);
        insertEntity.getParameters().get("field7").setValue(assetField7);
        SqlQuery insertQuery = insertEntity.toQuery();
        assertEquals(3, insertQuery.getParameters().size());
        assertEquals(
                List.of("id", "name", "field7"),
                insertQuery.getParameters().stream()
                        .map(Parameter::getName)
                        .collect(Collectors.toList()));
        //
        SqlEntity updateEntity = entities.loadEntity("dml/update-asset");
        assertNotNull(updateEntity);
        assertTrue(updateEntity.isCommand());
        assertTrue(updateEntity.getFields().isEmpty());
        assertEquals(2, updateEntity.getParameters().size());
        updateEntity.getParameters().get("id").setValue(assetId);
        updateEntity.getParameters().get("name").setValue(assetName + " updated");
        SqlQuery updateQuery = updateEntity.toQuery();
        assertEquals(2, updateQuery.getParameters().size());
        assertEquals(
                List.of("name", "id"),
                updateQuery.getParameters().stream()
                        .map(Parameter::getName)
                        .collect(Collectors.toList()));
        //
        SqlEntity deleteEntity = entities.loadEntity("dml/delete-asset");
        assertNotNull(deleteEntity);
        assertTrue(deleteEntity.isCommand());
        assertTrue(deleteEntity.getFields().isEmpty());
        assertEquals(1, deleteEntity.getParameters().size());
        deleteEntity.getParameters().get("id").setValue(assetId);
        SqlQuery deleteQuery = deleteEntity.toQuery();
        assertEquals(1, deleteQuery.getParameters().size());
        assertEquals("id", deleteQuery.getParameters().get(0).getName());

        insertQuery
                .start(Map.of())
                .thenApply(inserted -> {
                    assertNotNull(inserted);
                    assertEquals(1L, (long) inserted);
                    return updateQuery.start();
                })
                .thenCompose(Function.identity())
                .thenApply(updated -> {
                    assertNotNull(updated);
                    assertEquals(1L, (long) updated);
                    return deleteQuery.start();
                })
                .thenCompose(Function.identity())
                .thenAccept(deleted -> {
                    assertNotNull(deleted);
                    assertEquals(1L, (long) deleted);
                })
                .get();
    }

    @Test
    public void crudForeignParameters() throws Exception {
        int assetId = 654321;
        String assetName = "Septima 'crudForeignParameters' test asset";
        double assetField7 = 0.6d;
        SqlEntities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity insertEntity = entities.loadEntity("dml/insert-asset");
        assertNotNull(insertEntity);
        assertTrue(insertEntity.isCommand());
        assertTrue(insertEntity.getFields().isEmpty());
        assertEquals(3, insertEntity.getParameters().size());
        SqlQuery insertQuery = insertEntity.toQuery();
        assertEquals(3, insertQuery.getParameters().size());
        assertEquals(
                List.of("id", "name", "field7"),
                insertQuery.getParameters().stream()
                        .map(Parameter::getName)
                        .collect(Collectors.toList()));
        //
        SqlEntity updateEntity = entities.loadEntity("dml/update-asset");
        assertNotNull(updateEntity);
        assertTrue(updateEntity.isCommand());
        assertTrue(updateEntity.getFields().isEmpty());
        assertEquals(2, updateEntity.getParameters().size());
        SqlQuery updateQuery = updateEntity.toQuery();
        assertEquals(2, updateQuery.getParameters().size());
        assertEquals(
                List.of("name", "id"),
                updateQuery.getParameters().stream()
                        .map(Parameter::getName)
                        .collect(Collectors.toList()));
        //
        SqlEntity deleteEntity = entities.loadEntity("dml/delete-asset");
        assertNotNull(deleteEntity);
        assertTrue(deleteEntity.isCommand());
        assertTrue(deleteEntity.getFields().isEmpty());
        assertEquals(1, deleteEntity.getParameters().size());
        SqlQuery deleteQuery = deleteEntity.toQuery();
        assertEquals(1, deleteQuery.getParameters().size());
        assertEquals("id", deleteQuery.getParameters().get(0).getName());

        insertQuery
                .start(Map.of(
                        "id", assetId,
                        "name", assetName,
                        "field7", assetField7
                ))
                .thenApply(inserted -> {
                    assertNotNull(inserted);
                    assertEquals(1L, (long) inserted);
                    return updateQuery.start(
                            Map.of(
                                    "name", assetName + " updated",
                                    "id", assetId
                            ));
                })
                .thenCompose(Function.identity())
                .thenApply(updated -> {
                    assertNotNull(updated);
                    assertEquals(1L, (long) updated);
                    return deleteQuery.start(
                            Map.of(
                                    "id", assetId
                            ));
                })
                .thenCompose(Function.identity())
                .thenAccept(deleted -> {
                    assertNotNull(deleted);
                    assertEquals(1L, (long) deleted);
                })
                .get();
    }

    @Test
    public void storedProcedureOwnParameters() {
    }

    @Test
    public void storedProcedureForeignParameters() {
    }

}
