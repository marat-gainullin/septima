package com.septima.metadata;

import com.septima.GenericType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.*;

public class FieldsTest {

    @Test
    public void fieldConstructors() {
        EntityField entityField = new EntityField("test");
        assertEquals("test", entityField.getName());
        assertEquals(GenericType.STRING, entityField.getType());
        assertEquals("test", entityField.getOriginalName());
        assertNull(entityField.getDescription());
        assertTrue(entityField.isNullable());
    }

    @Test
    public void jdbcColumnConstructor() {
        JdbcColumn column = new JdbcColumn(
                "test",
                "test desc",
                null,
                "test_table",
                "VARCHAR",
                true,
                true,
                new ForeignKey(
                        "test_schema", "test_table", "test", "test_fk",
                        ForeignKey.ForeignKeyRule.CASCADE, ForeignKey.ForeignKeyRule.CASCADE, true,
                        "test_schema", "test_table", "test", "test_pk"
                ),
                10,
                1000,
                2,
                true,
                "test_schema",
                Types.VARCHAR
        );
        assertEquals(10, column.getSize());
        assertEquals(1000, column.getScale());
        assertEquals(2, column.getPrecision());
        assertEquals(true, column.isSigned());
        assertEquals("test_schema", column.getSchema());
        assertEquals(Types.VARCHAR, column.getJdbcType());
        assertEquals("VARCHAR", column.getRdbmsType());
    }

    @Test
    public void jdbcColumnToString() {
        JdbcColumn column = new JdbcColumn(
                "test",
                "test desc",
                null,
                "test_table",
                "VARCHAR",
                true,
                true,
                new ForeignKey(
                        "test_schema", "test_table", "test", "test_fk",
                        ForeignKey.ForeignKeyRule.CASCADE, ForeignKey.ForeignKeyRule.CASCADE, true,
                        "test_schema", "test_table", "test", "test_pk"
                ),
                10,
                1000,
                2,
                true,
                "test_schema",
                Types.VARCHAR
        );
        assertEquals("test_schema.test_table.test (test desc), primary key, foreign key transform test_schema.test_table.test, VARCHAR, size 10, precision 2, scale 1000, signed, nullable", column.toString());
    }

    @Test
    public void emptyJdbcColumnToString() {
        JdbcColumn column = new JdbcColumn(
                "test",
                null,
                null,
                null,
                "VARCHAR",
                false,
                false,
                null,
                10,
                1000,
                2,
                false,
                null,
                Types.VARCHAR
        );
        assertEquals("test, VARCHAR, size 10, precision 2, scale 1000", column.toString());
    }

    @Test
    public void jdbcColumnOriginalNameToString() {
        JdbcColumn column = new JdbcColumn(
                "test",
                "test desc",
                "TEST",
                "test_table",
                "VARCHAR",
                true,
                true,
                new ForeignKey(
                        "test_schema", "test_table", "test", "test_fk",
                        ForeignKey.ForeignKeyRule.CASCADE, ForeignKey.ForeignKeyRule.CASCADE, true,
                        "test_schema", "test_table", "test", "test_pk"
                ),
                10,
                1000,
                2,
                true,
                "test_schema",
                Types.VARCHAR
        );
        assertEquals("test_schema.test_table.TEST (test desc), primary key, foreign key transform test_schema.test_table.test, VARCHAR, size 10, precision 2, scale 1000, signed, nullable", column.toString());
    }

    @Test
    public void fieldToString() {
        EntityField entityField = new EntityField(
                "test",
                "test desc",
                null,
                "test_table",
                GenericType.DATE,
                true,
                true,
                new ForeignKey(
                        "test_schema", "test_table", "test", "test_fk",
                        ForeignKey.ForeignKeyRule.CASCADE, ForeignKey.ForeignKeyRule.CASCADE, true,
                        "test_schema", "test_table", "test", "test_pk"
                )
        );
        assertEquals("test_table.test (test desc), primary key, foreign key test_schema.test_table.test, DATE, nullable", entityField.toString());
    }

    @Test
    public void emptyFieldToString() {
        EntityField entityField = new EntityField("test");
        assertEquals("test, STRING, nullable", entityField.toString());
    }

    @Test
    public void fieldOriginalNameToString() {
        EntityField entityField = new EntityField(
                "test",
                "test desc",
                "TEST",
                "test_table",
                GenericType.DATE,
                true,
                true,
                new ForeignKey(
                        "test_schema", "test_table", "test", "test_fk",
                        ForeignKey.ForeignKeyRule.CASCADE, ForeignKey.ForeignKeyRule.CASCADE, true,
                        "test_schema", "test_table", "test", "test_pk"
                )
        );
        assertEquals("test_table.TEST (test desc), primary key, foreign key test_schema.test_table.test, DATE, nullable", entityField.toString());
    }
}
