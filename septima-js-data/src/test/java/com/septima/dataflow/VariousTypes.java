package com.septima.dataflow;

import com.septima.TestDataSource;
import com.septima.changes.EntityInsert;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.NamingException;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class VariousTypes {

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    private static void dataFlow(Supplier<Map<String, Object>> values) throws Exception {
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
                    assertFalse(data.isEmpty());
                    return entity.getDatabase().commit(entities.bindChanges(List.of(
                            new EntityInsert(entity.getName(), values.get())
                    )));
                }).thenCompose(Function.identity())
                .thenAccept(inserted -> assertEquals(1L, (long) inserted))
                .get();
    }

    @Test
    public void stringsDataFlow() throws Exception {
        dataFlow(() -> Map.ofEntries(
                Map.entry("s1", "val 1"),
                Map.entry("s2", "val 2"),
                Map.entry("s3", "val 3"),
                Map.entry("s4", "val 4"),
                Map.entry("s5", "val 5"),
                Map.entry("iden2", "123456")
        ));
    }

    @Test
    public void booleansTimesDataFlow() throws Exception {
        dataFlow(() -> Map.ofEntries(
                Map.entry("b1", true)
        ));
    }

    @Test
    public void datesTimesDataFlow() throws Exception {
        dataFlow(() -> Map.ofEntries(
                Map.entry("dt1", new Date()),
                Map.entry("dt2", new Date()),
                Map.entry("dt3", new Date())
        ));
    }

    @Test
    public void binariesDataFlow() throws Exception {
        dataFlow(() -> Map.ofEntries(
                Map.entry("bin1", "bin val 1".getBytes(StandardCharsets.US_ASCII)),
                Map.entry("bin2", "bin val 2".getBytes(StandardCharsets.US_ASCII)),
                Map.entry("bin3", "bin val 3".getBytes(StandardCharsets.US_ASCII)),
                Map.entry("bin4", "bin val 4".getBytes(StandardCharsets.US_ASCII))
        ));
    }

    @Test
    public void shortsDataFlow() throws Exception {
        dataFlow(() -> Map.ofEntries(
                Map.entry("n1", (short) 1),
                Map.entry("n2", (short) 2),
                Map.entry("n3", (short) 3),
                Map.entry("n4", (short) 4),
                Map.entry("n5", (short) 5),
                Map.entry("n6", (short) 6),
                Map.entry("n7", (short) 7),
                Map.entry("n8", (short) 8),
                Map.entry("n9", (short) 9)
        ));
    }

    @Test
    public void intsDataFlow() throws Exception {
        dataFlow(() -> Map.ofEntries(
                Map.entry("n1", 1),
                Map.entry("n2", 2),
                Map.entry("n3", 3),
                Map.entry("n4", 4),
                Map.entry("n5", 5),
                Map.entry("n6", 6),
                Map.entry("n7", 7),
                Map.entry("n8", 8),
                Map.entry("n9", 9)
        ));
    }

    @Test
    public void longsDataFlow() throws Exception {
        dataFlow(() -> Map.ofEntries(
                Map.entry("n1", 1L),
                Map.entry("n2", 2L),
                Map.entry("n3", 3L),
                Map.entry("n4", 4L),
                Map.entry("n5", 5L),
                Map.entry("n6", 6L),
                Map.entry("n7", 7L),
                Map.entry("n8", 8L),
                Map.entry("n9", 9L)
        ));
    }

    @Test
    public void floatsDataFlow() throws Exception {
        dataFlow(() -> Map.ofEntries(
                Map.entry("n1", 1f),
                Map.entry("n2", 2f),
                Map.entry("n3", 3f),
                Map.entry("n4", 4f),
                Map.entry("n5", 5f),
                Map.entry("n6", 6f),
                Map.entry("n7", 7f),
                Map.entry("n8", 8f),
                Map.entry("n9", 9f)
        ));
    }

    @Test
    public void doublesDataFlow() throws Exception {
        dataFlow(() -> Map.ofEntries(
                Map.entry("n1", 1d),
                Map.entry("n2", 2d),
                Map.entry("n3", 3d),
                Map.entry("n4", 4d),
                Map.entry("n5", 5d),
                Map.entry("n6", 6d),
                Map.entry("n7", 7d),
                Map.entry("n8", 8d),
                Map.entry("n9", 9d)
        ));
    }
}
