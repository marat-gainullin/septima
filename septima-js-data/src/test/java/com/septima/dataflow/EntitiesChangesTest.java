package com.septima.dataflow;

import com.septima.Database;
import com.septima.TestDataSource;
import com.septima.changes.EntityCommand;
import com.septima.changes.EntityDelete;
import com.septima.changes.EntityInsert;
import com.septima.changes.EntityUpdate;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.NamingException;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EntitiesChangesTest {

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    @Test
    public void crudChanges() throws Exception {
        long k_id;
        long t_id;
        long g_id;
        k_id = t_id = g_id = 123456;
        String k_name = "k_name-" + k_id;
        String t_name = "t_name-" + t_id;
        String g_name = "g_name-" + g_id;
        SqlEntities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity compoundEntity = entities.loadEntity("changes/asset-groups-types-kinds");
        compoundEntity.toQuery().requestData()
                .thenApply(data -> {
                    assertTrue(data.size() > 1);
                    return compoundEntity.getDatabase().commit(entities.bindChanges(List.of(
                            new EntityInsert(compoundEntity.getName(), Map.of(
                                    "k_id", k_id,
                                    "k_name", k_name,
                                    "t_id", t_id,
                                    "t_name", t_name,
                                    "g_id", g_id,
                                    "g_name", g_name
                            ))
                    )));
                }).thenCompose(Function.identity())
                .thenApply(inserted -> {
                    assertEquals(3L, (long) inserted);
                    return compoundEntity.getDatabase().commit(entities.bindChanges(List.of(
                            new EntityUpdate(compoundEntity.getName(),
                                    Map.of(
                                            "k_id", k_id,
                                            "t_id", t_id,
                                            "g_id", g_id
                                    ),
                                    Map.of(
                                            "k_name", k_name + " updated",
                                            "t_name", t_name + " updated",
                                            "g_name", g_name + " updated"
                                    )
                            ),
                            new EntityDelete(compoundEntity.getName(), Map.of(
                                    "k_id", k_id,
                                    "t_id", t_id,
                                    "g_id", g_id
                            ))
                    )));
                }).thenCompose(Function.identity())
                .thenAccept(affected -> assertEquals(6L, (long) affected))
                .get();
    }

    @Test
    public void writableTablesRestriction() throws Exception {
        long k_id;
        long t_id;
        long g_id;
        k_id = t_id = g_id = 223456;
        String k_name = "k_name-" + k_id;
        String t_name = "t_name-" + t_id;
        String g_name = "g_name-" + g_id;
        SqlEntities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity compoundEntity = entities.loadEntity("changes/asset-groups-types-kinds-writable-restricted");
        compoundEntity.toQuery().requestData(Map.of())
                .thenApply(data -> {
                    assertTrue(data.size() > 1);
                    return compoundEntity.getDatabase().commit(entities.bindChanges(List.of(
                            new EntityInsert(compoundEntity.getName(), Map.of(
                                    "k_id", k_id,
                                    "k_name", k_name,
                                    "t_id", t_id,
                                    "t_name", t_name,
                                    "g_id", g_id,
                                    "g_name", g_name
                            ))
                    )));
                }).thenCompose(Function.identity())
                .thenApply(inserted -> {
                    assertEquals(1L, (long) inserted);
                    return compoundEntity.getDatabase().commit(entities.bindChanges(List.of(
                            new EntityUpdate(compoundEntity.getName(),
                                    Map.of(
                                            "k_id", k_id,
                                            "t_id", t_id,
                                            "g_id", g_id
                                    ),
                                    Map.of(
                                            "k_name", k_name + " updated",
                                            "t_name", t_name + " updated",
                                            "g_name", g_name + " updated"
                                    )
                            ),
                            new EntityDelete(compoundEntity.getName(), Map.of(
                                    "k_id", k_id,
                                    "t_id", t_id,
                                    "g_id", g_id
                            ))
                    )));
                }).thenCompose(Function.identity())
                .thenAccept(affected -> assertEquals(2L, (long) affected))
                .get();
    }

    @Test
    public void commandChanges() throws Exception {
        long assetId = 823456;
        String assetName = "asset-" + assetId;
        double assetField7 = 0.2d;
        Database database = Database.of(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME));
        SqlEntities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        database.commit(entities.bindChanges(List.of(
                new EntityCommand("dml/insert-asset",
                        Map.of(
                                "field7", assetField7,
                                "name", assetName,
                                "id", assetId
                        )
                ),
                new EntityCommand("dml/update-asset",
                        Map.of(
                                "id", assetId,
                                "name", assetName + " updated"
                        )
                ),
                new EntityCommand("dml/delete-asset",
                        Map.of(
                                "id", assetId
                        )
                )
        )))
                .thenAccept(affected -> assertEquals(3L, (long) affected)).get();
    }
}
