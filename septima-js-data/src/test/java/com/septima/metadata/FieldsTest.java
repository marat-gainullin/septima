package com.septima.metadata;

import com.septima.DataTypes;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.*;

public class FieldsTest {

    @Test
    public void fieldConstructors() {
        Field field = new Field("test");
        assertEquals("test", field.getName());
        assertEquals(DataTypes.STRING_TYPE_NAME, field.getType());
        assertEquals("test", field.getOriginalName());
        assertNull(field.getDescription());
        assertTrue(field.isNullable());
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
        assertEquals("test_schema", column.getSchemaName());
        assertEquals(Types.VARCHAR, column.getJdbcType());
        assertEquals("VARCHAR", column.getType());
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
        Field field = new Field(
                "test",
                "test desc",
                null,
                "test_table",
                DataTypes.DATE_TYPE_NAME,
                true,
                true,
                new ForeignKey(
                        "test_schema", "test_table", "test", "test_fk",
                        ForeignKey.ForeignKeyRule.CASCADE, ForeignKey.ForeignKeyRule.CASCADE, true,
                        "test_schema", "test_table", "test", "test_pk"
                )
        );
        assertEquals("test_table.test (test desc), primary key, foreign key transform test_schema.test_table.test, Date, nullable", field.toString());
    }

    @Test
    public void emptyFieldToString() {
        Field field = new Field("test");
        assertEquals("test, String, nullable", field.toString());
    }

    @Test
    public void fieldOriginalNameToString() {
        Field field = new Field(
                "test",
                "test desc",
                "TEST",
                "test_table",
                DataTypes.DATE_TYPE_NAME,
                true,
                true,
                new ForeignKey(
                        "test_schema", "test_table", "test", "test_fk",
                        ForeignKey.ForeignKeyRule.CASCADE, ForeignKey.ForeignKeyRule.CASCADE, true,
                        "test_schema", "test_table", "test", "test_pk"
                )
        );
        assertEquals("test_table.TEST (test desc), primary key, foreign key transform test_schema.test_table.test, Date, nullable", field.toString());
    }
}
