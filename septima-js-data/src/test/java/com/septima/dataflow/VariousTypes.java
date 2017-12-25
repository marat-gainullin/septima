package com.septima.dataflow;

import com.septima.NamedValue;
import com.septima.TestDataSource;
import com.septima.changes.Insert;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import org.junit.Test;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
                            new Insert(entity.getName(), List.of(
                                    new NamedValue("n1", 1),
                                    new NamedValue("n2", 2),
                                    new NamedValue("n3", 3),
                                    new NamedValue("n4", 4),
                                    new NamedValue("n5", 5),
                                    new NamedValue("n6", 6),
                                    new NamedValue("n7", 7),
                                    new NamedValue("s1", "val 1"),
                                    new NamedValue("s2", "val 2"),
                                    new NamedValue("s3", "val 3"),
                                    new NamedValue("s4", "val 4"),
                                    new NamedValue("s5", "val 5"),
                                    new NamedValue("n8", 8),
                                    new NamedValue("n9", 9),
                                    new NamedValue("b1", true),
                                    new NamedValue("iden2", "123456"),
                                    //new NamedValue("bin1", "bin val 1"),
                                    //new NamedValue("bin2", "bin val 2"),
                                    new NamedValue("dt1", new Date()),
                                    new NamedValue("dt2", new Date()),
                                    new NamedValue("dt3", new Date())
                                    //new NamedValue("bin3", "bin val 3"),
                                    //new NamedValue("bin4", "bin val 4")
                            ))
                    )));
                }).thenCompose(Function.identity())
                .thenAccept(inserted -> assertEquals(1L, (long) inserted))
                .get();
    }
}
