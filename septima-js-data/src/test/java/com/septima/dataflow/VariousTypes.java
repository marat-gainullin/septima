package com.septima.dataflow;

import com.septima.TestDataSource;
import com.septima.changes.Insert;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import org.junit.Test;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.*;

public class VariousTypes {

    @Test
    public void dataFlow() throws Exception {
        SqlEntities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("various-types/various-types");
        assertNotNull(entity);
        assertEquals(24, entity.getFields().size());
        entity.toQuery()
                .requestData()
                .thenApply(data -> {
                    assertTrue(data.isEmpty());
                    return entity.getDatabase().commit(entities.bindChanges(List.of(
                            new Insert(entity.getName(), Map.ofEntries(
                                    Map.entry("n1", 1),
                                    Map.entry("n2", 2),
                                    Map.entry("n3", 3),
                                    Map.entry("n4", 4),
                                    Map.entry("n5", 5),
                                    Map.entry("n6", 6),
                                    Map.entry("n7", 7),
                                    Map.entry("s1", "val 1"),
                                    Map.entry("s2", "val 2"),
                                    Map.entry("s3", "val 3"),
                                    Map.entry("s4", "val 4"),
                                    Map.entry("s5", "val 5"),
                                    Map.entry("n8", 8),
                                    Map.entry("n9", 9),
                                    Map.entry("b1", true),
                                    Map.entry("iden2", "123456"),
                                    //Map.entry("bin1", "bin val 1"),
                                    //Map.entry("bin2", "bin val 2"),
                                    Map.entry("dt1", new Date()),
                                    Map.entry("dt2", new Date()),
                                    Map.entry("dt3", new Date())
                                    //Map.entry("bin3", "bin val 3"),
                                    //Map.entry("bin4", "bin val 4")
                            ))
                    )));
                }).thenCompose(Function.identity())
                .thenAccept(inserted -> assertEquals(1L, (long) inserted))
                .get();
    }
}
