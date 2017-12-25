package com.septima.sqldrivers;

import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.*;

public class GenericDriverTest {

    @Test
    public void plugins() {
        assertNotNull(SqlDriver.of("db2"));
        assertNotNull(SqlDriver.of("h2"));
        assertNotNull(SqlDriver.of("jtds"));
        assertNotNull(SqlDriver.of("mysql"));
        assertNotNull(SqlDriver.of("oracle"));
        assertNotNull(SqlDriver.of("postresql"));
        assertNotNull(SqlDriver.of("Whatever")); // Generic implementation
    }

    @Test
    public void defaultImplemented() {
        SqlDriver driver = new SqlDriver();
        assertEquals("Generic", driver.getDialect());
        assertFalse(driver.isConstraintsDeferrable());
        assertEquals(Integer.MAX_VALUE, driver.getTypesResolver().resolveSize("Whatever", Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, driver.getTypesResolver().resolveSize("AnyType", Integer.MAX_VALUE));
        assertArrayEquals(new String[]{"alter table schema.table drop column field"}, driver.getSql4FieldDrop("schema", "table", "field"));
        assertEquals("\"", "" + driver.getEscape());
        assertEquals("schema.table", driver.makeFullName("schema", "table"));
        assertEquals("\"schem a\".\"ta ble\"", driver.makeFullName("schem a", "ta ble"));
        assertEquals("sche\"m a", driver.unescapeNameIfNeeded("\"sche\"\"m a\""));
    }

    @Test
    public void defaultUnimplemented() throws SQLException {
        SqlDriver driver = new SqlDriver();
        assertNull(driver.getSql4GetSchema());
        assertNull(driver.getSql4CreateSchema("Whatever", "Whatever"));
        assertNull(driver.getSql4CreateTableComment("Whatever", "Whatever", "Whatever"));
        assertNull(driver.getSql4DropTable("Whatever", "Whatever"));
        assertNull(driver.getSql4DropFkConstraint("Whatever", null));
        assertNull(driver.getSql4DropPkConstraint("Whatever", null));
        assertNull(driver.getSql4CreateFkConstraint("Whatever", List.of()));
        assertNull(driver.getSql4EmptyTableCreation("Whatever", "Whatever", "Whatever"));
        assertNull(driver.getSql4FieldDefinition(null));
        assertNull(driver.geometryFromWkt(null, null, null));
        assertNull(driver.geometryToWkt(null, 1, null));
        assertArrayEquals(new String[]{}, driver.getSqls4FieldAdd("Whatever", "Whatever", null));
        assertArrayEquals(new String[]{}, driver.getSqls4FieldModify("Whatever", "Whatever", null, null));
        assertArrayEquals(new String[]{}, driver.getSqls4FieldRename("Whatever", "Whatever", null, null));
        assertArrayEquals(new String[]{}, driver.getSqls4CreateColumnComment("Whatever", "Whatever", "Whatever", "Whatever"));
        assertArrayEquals(new String[]{}, driver.getSqls4CreatePkConstraint("Whatever", List.of()));
    }
}
