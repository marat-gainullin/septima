package com.septima.dataflow;

import com.septima.TestDataSource;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GeometryTest {

    @Test
    public void requestDataWithGeometry() throws Exception {
        SqlEntities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity compoundEntity = entities.loadEntity("geometry/data-with-geometry");
        compoundEntity.toQuery().requestData()
                .thenAccept(data -> {
                    try {
                        assertEquals(1, data.size());
                        Map<String, Object> datum = data.iterator().next();
                        assertTrue(datum.get("the_geom") instanceof String);
                        WKTReader wktReader = new WKTReader();
                        Geometry the_geom = wktReader.read((String) datum.get("the_geom"));
                        assertTrue(the_geom instanceof MultiLineString);
                    } catch (ParseException ex) {
                        throw new IllegalStateException(ex);
                    }
                }).get();
    }

    @Test
    public void insertGeometry() {
        SqlEntities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity insertEntity = entities.loadEntity("geometry/insert-administrative");
        assertNotNull(insertEntity);
        assertTrue(insertEntity.isCommand());
        assertTrue(insertEntity.getFields().isEmpty());
        assertEquals(4, insertEntity.getParameters().size());
        insertEntity.toQuery()
                .start(Map.of(
                        "id", "2345",
                        "shape", "LINESTRING (-75.784802 39.819017, -75.786246 39.818865, -75.787034 39.8199309, -75.786044 39.829671, -75.786022 39.829678, -75.785328 39.82978)",
                        "name", "New county",
                        "admLevel", 5

                ))
                .thenAccept(inserted -> assertEquals(1L, (long) inserted));
    }
}
