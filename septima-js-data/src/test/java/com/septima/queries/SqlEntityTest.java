package com.septima.queries;

import com.septima.Database;
import com.septima.metadata.Parameter;
import com.septima.TestDataSource;
import com.septima.dataflow.DataProvider;
import com.septima.entities.SqlEntity;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.NamingException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * @author mg
 */
public class SqlEntityTest {

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    @Test(expected = IllegalStateException.class)
    public void sqlEntityEmptySql() throws Exception {
        SqlEntity entity = new SqlEntity(Database.of(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME), 32, true, 1),
                "",
                null,
                "",
                false,
                false,
                false,
                false,
                "",
                DataProvider.NO_PAGING_PAGE_SIZE,
                Map.of(), Map.of(),
                Set.of(), Set.of(), Set.of()
        );
        entity.toQuery();
    }

    @Test
    public void sqlEntityCustomSql() throws Exception {
        SqlEntity entity = new SqlEntity(Database.of(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME), 32, true, 1),
                "select f1, f2 from table",
                "select f1, f2::json from table",
                "",
                false,
                false,
                false,
                false,
                "",
                DataProvider.NO_PAGING_PAGE_SIZE,
                Map.of(), Map.of(),
                Set.of(), Set.of(), Set.of()
        );
        SqlQuery query = entity.toQuery();
        assertEquals("select f1, f2::json from table", query.getSqlClause());
    }

    @Test
    public void sqlEntityStructure() throws Exception {
        Database database = Database.of(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME), 32, true, 1);
        SqlEntity entity = new SqlEntity(database,
                "select f1, f2 from table",
                "select f1, f2::json from table",
                "testEntity",
                true,
                true,
                true,
                true,
                "Awesome sql based entity",
                64,
                Map.of(), Map.of(),
                Set.of(), Set.of(), Set.of()
        );
        assertSame(database, entity.getDatabase());
        assertEquals("select f1, f2 from table", entity.getSqlText());
        assertEquals("select f1, f2::json from table", entity.getCustomSqlText());
        assertTrue(entity.isReadonly());
        assertTrue(entity.isCommand());
        assertTrue(entity.isProcedure());
        assertTrue(entity.isPublicAccess());
        assertEquals("testEntity", entity.getName());
        assertEquals("Awesome sql based entity", entity.getTitle());
        assertEquals(64, entity.getPageSize());
        assertEquals(Map.of(), entity.getParameters());
        assertEquals(Map.of(), entity.getFields());
        assertEquals(Set.of(), entity.getWritable());
        assertEquals(Set.of(), entity.getReadRoles());
        assertEquals(Set.of(), entity.getWriteRoles());

        SqlQuery query = entity.toQuery();
        assertEquals("select f1, f2::json from table", query.getSqlClause());
        assertEquals("testEntity", query.getEntityName());
        assertEquals(64, query.getPageSize());
        assertTrue(query.isProcedure());
    }

    @Test
    public void namedParametersExtraction() throws Exception {
        SqlEntity entity = new SqlEntity(
                Database.of(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME), 32, true, 1),
                "select a.id::json, /*multil/i*ne \n com \r ment * /text :pp1 */'jj :pp2 ww' txt from assets a\n" +
                        "-- line :comment text\r" +
                        "-- line :comment text\n" +
                        "-- line :comment text\r\n" +
                        "-- line :comment text\n\r" +
                        "where a.id = :id and a.o < :o and a.g < :o");
        assertEquals(2, entity.getParameters().size());
        assertArrayEquals(new String[]{
                "id",
                "o"
        }, entity.getParameters().values().stream()
                .map(Parameter::getName)
                .collect(Collectors.toList())
                .toArray(new String[]{}));
    }

    @Test
    public void namedParametersToJdbcParameters() throws Exception {
        SqlEntity entity = new SqlEntity(
                Database.of(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME), 32, true, 1),
                "select a.id::json ajson, /*multil/i*ne \n" +
                        " com \r" +
                        " ment * /'jj '':pp ww' txt from assets a\n" +
                        "-- line :comment text\r" +
                        "-- line :comment text\n" +
                        "-- line :comment text\r\n" +
                        "where a.id = :id and a.o < :o and a.g < :o");
        SqlQuery query = entity.toQuery();
        assertEquals(3, query.getParameters().size());
        assertEquals("select a.id::json ajson, /*multil/i*ne \n" +
                " com \r" +
                " ment * /'jj '':pp ww' txt from assets a\n" +
                "-- line :comment text\r" +
                "-- line :comment text\n" +
                "-- line :comment text\r\n" +
                "where a.id = ? and a.o < ? and a.g < ?", query.getSqlClause());
        assertArrayEquals(new String[]{
                "id",
                "o",
                "o"
        }, query.getParameters().stream()
                .map(Parameter::getName)
                .collect(Collectors.toList())
                .toArray(new String[]{}));
    }

}
