package com.septima.queries;

import com.septima.Entities;
import com.septima.Parameter;
import com.septima.TestDataSource;
import com.septima.DataTypes;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import com.septima.entities.SqlEntityCyclicReferenceException;
import com.septima.metadata.Field;
import net.sf.jsqlparser.UncheckedJSqlParserException;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.NamingException;
import java.io.File;
import java.io.UncheckedIOException;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author mg
 */
public class SqlEntitiesTest {

    private static String rn2n(String withRn) {
        return withRn.replace("\r\n", "\n").replace("\n\r", "\n").replace("\r", "\n");
    }

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    @Test(expected = IllegalStateException.class)
    public void badDataSource() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/bad-data-source");
    }

    /**
     * If sql text is unparsable, then no special action are needed.
     * User should provide parsable sql text and use customSql transform get what he wants.
     */
    @Test(expected = UncheckedJSqlParserException.class)
    public void unparsableSqlText() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/unparsable");
    }

    @Test(expected = UncheckedJSqlParserException.class)
    public void withUnparsableSubEntity() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/inline/with-unparsable-sub-entity");
    }

    @Test(expected = SqlEntityCyclicReferenceException.class)
    public void cyclicHashRefsTwoElements() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/inline/cyclic/a");
    }

    @Test(expected = SqlEntityCyclicReferenceException.class)
    public void cyclicHashRefsThreeElements() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/inline/cyclic/a1");
    }

    @Test(expected = IllegalStateException.class)
    public void emptyEntity() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/empty");
    }

    @Test(expected = IllegalStateException.class)
    public void withEmptySubEntity() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/empty");
    }

    @Test(expected = UncheckedIOException.class)
    public void absentEntity() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/absentEntity");
    }

    @Test(expected = UncheckedIOException.class)
    public void directoryInsteadOfEntitySqlFile() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/directory-entity");
    }

    @Test(expected = UncheckedIOException.class)
    public void withAbsentSubEntity() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/inline/delete-in-select/with-absent-sub-entity");
    }

    @Test(expected = IllegalStateException.class)
    public void withInlinedDelete() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/inline/delete-in-select/with-delete");
    }

    @Test
    public void ethalonJsonContent() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/ethalon");
        assertNotNull(entity);
        assertEquals("entities/ethalon", entity.getEntityName());
        assertEquals("Custom orders", entity.getTitle());
        assertEquals("select o.*, co.body::json from orders o inner join customOrders co on(o.id = co.order_id)", entity.getCustomSqlText());
        assertEquals("Select o.order_id id, o.amount amt, o.good goodik\n" +
                ", o.customer orderer, o.field1 other \n" +
                "From goodOrder o\n" +
                " Inner Join customOrders co on (o.id = co.order_id)", rn2n(entity.getSqlText()));
        assertTrue(entity.isCommand());
        assertTrue(entity.isProcedure());
        assertTrue(entity.isProcedure());
        assertTrue(entity.isPublicAccess());
        assertEquals(10, entity.getPageSize());
        // Parameters
        assertEquals(1, entity.getParameters().size());
        Parameter quantity = entity.getParameters().get("quantity");
        assertNotNull(quantity);
        assertEquals(DataTypes.NUMBER_TYPE_NAME, quantity.getType());
        assertEquals("Quantity desc", quantity.getDescription());
        assertEquals("Simon", quantity.getValue());
        assertEquals(Parameter.Mode.InOut, quantity.getMode());
        // Select o.order_id id, o.amount amt, o.good goodik, o.customer orderer, o.field1 other From ...
        // Fields
        assertEquals(5, entity.getFields().size());
        Field order_id = entity.getFields().get("id");
        assertNotNull(order_id);
        assertEquals("id", order_id.getName());
        assertEquals("ORDER_ID", order_id.getOriginalName());
        assertTrue(order_id.isPk());
        assertEquals("Ключик", order_id.getDescription());
        assertEquals("GOODORDER", order_id.getTableName());
        assertEquals("Number", order_id.getType());
        assertNull(order_id.getFk());
        Field amount = entity.getFields().get("amt");
        assertNotNull(amount);
        assertEquals("amt", amount.getName());
        assertEquals("AMOUNT", amount.getOriginalName());
        assertFalse(amount.isPk());
        assertEquals("Количество товара", amount.getDescription());
        assertEquals("GOODORDER", amount.getTableName());
        assertEquals("Number", amount.getType());
        assertNull(amount.getFk());
        Field good = entity.getFields().get("goodik");
        assertNotNull(good);
        assertEquals("goodik", good.getName());
        assertEquals("GOOD", good.getOriginalName());
        assertEquals("Заказанный товар", good.getDescription());
        assertEquals("GOODORDER", good.getTableName());
        assertEquals("Number", good.getType());
        assertNotNull(good.getFk());
        Field customer = entity.getFields().get("orderer");
        assertNotNull(customer);
        assertEquals("orderer", customer.getName());
        assertEquals("CUSTOMER", customer.getOriginalName());
        assertEquals("Заказчик", customer.getDescription());
        assertEquals("GOODORDER", customer.getTableName());
        assertEquals("Number", customer.getType());
        assertNotNull(customer.getFk());
        Field field1 = entity.getFields().get("other");
        assertNotNull(field1);
        assertEquals("other", field1.getName());
        assertEquals("FIELD1", field1.getOriginalName());
        assertEquals("", field1.getDescription());
        assertEquals("GOODORDER", field1.getTableName());
        assertNull(field1.getType());
        assertNull(field1.getFk());

        assertEquals(Set.of("orders"), entity.getWritable());
        assertEquals(Set.of("disp", "mech"), entity.getReadRoles());
        assertEquals(Set.of("disp", "mech"), entity.getWriteRoles());
    }

    @Test
    public void ethalonJsonMergedContent() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/ethalon-overriden-fields");
        assertNotNull(entity);
        // Fields
        assertEquals(6, entity.getFields().size());
        // Surrogate field
        Field id = entity.getFields().get("id");
        assertNotNull(id);
        assertEquals("id", id.getName());
        assertEquals("id", id.getOriginalName());
        assertTrue(id.isPk());
        assertEquals("Newly added key", id.getDescription());
        assertEquals("Number", id.getType());
        assertNull(id.getFk());
        // Overriden field
        Field order_id = entity.getFields().get("order_id");
        assertNotNull(order_id);
        assertEquals("order_id", order_id.getName());
        assertEquals("ORDER_ID", order_id.getOriginalName());
        assertFalse(order_id.isPk());
        assertEquals("Disabled key", order_id.getDescription());
        assertEquals("un-existent-table", order_id.getTableName());
        assertFalse(order_id.isNullable());
        assertEquals(DataTypes.STRING_TYPE_NAME, order_id.getType());
        assertNotNull(order_id.getFk());
    }

    @Test
    public void simpleInline() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        assertEquals(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME), entities.getDefaultDataSource());
        SqlEntity entity = entities.loadEntity("entities/inline/simple/a");
        assertNotNull(entity);
        assertEquals("Select * \n" +
                "From (Select * \n" +
                "From (Select * \n" +
                "From assets asts\n" +
                " Where asts.id = :a_id) c) entities_inline_simple_b\n" +
                ", (Select * \n" +
                "From (Select * \n" +
                "From (Select * \n" +
                "From assets asts\n" +
                " Where asts.id = :a_id) c) entities_inline_simple_b) absB", rn2n(entity.getSqlText()));
    }

    @Test
    public void variousCase() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/case/with-various-case");
        assertEquals(4, entity.getFields().size());
        Field mdent_id = entity.getFields().get("mdent_id");
        assertEquals("MDENt_ID", mdent_id.getName());
        assertEquals(DataTypes.STRING_TYPE_NAME, mdent_id.getType());
        assertTrue(mdent_id.isPk());
        Field mdent_name = entity.getFields().get("mdent_name");
        assertEquals("MDENT_NAME", mdent_name.getName());
        assertEquals(DataTypes.STRING_TYPE_NAME, mdent_name.getType());
        assertFalse(mdent_name.isPk());
        Field mdent_type = entity.getFields().get("mdent_type");
        assertEquals("MDENT_TYPe", mdent_type.getName());
        assertEquals(DataTypes.NUMBER_TYPE_NAME, mdent_type.getType());
        assertFalse(mdent_type.isPk());
        Field mdent_order = entity.getFields().get("mdent_order");
        assertEquals("MDENT_ORDER", mdent_order.getName());
        assertEquals(DataTypes.NUMBER_TYPE_NAME, mdent_order.getType());
        assertFalse(mdent_order.isPk());
    }

    @Test
    public void multiplePrimaryKeys() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/keys/with-multiple-primary-keys");
        Field mdent_id = entity.getFields().get("mdent_id");
        assertNotNull(mdent_id);
        assertTrue(mdent_id.isPk());
        Field mdlog_id = entity.getFields().get("mdlog_id");
        assertNotNull(mdlog_id);
        assertTrue(mdlog_id.isPk());
    }

    @Test
    public void allColumnsTwoTables() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/all-columns/two-tables");
        assertNotNull(entity);
        assertEquals(7, entity.getFields().size());
        assertTrue(entity.getFields().containsKey("id"));
        assertTrue(entity.getFields().get("id").isPk());
        assertTrue(entity.getFields().containsKey("f1"));
        assertTrue(entity.getFields().containsKey("f2"));
        assertTrue(entity.getFields().containsKey("f3"));
        assertTrue(entity.getFields().containsKey("fielda"));
        assertTrue(entity.getFields().containsKey("fieldb"));
        assertTrue(entity.getFields().containsKey("fieldc"));
    }

    @Test
    public void allColumnsOneTableOneFieldFromOtherTable() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/all-columns/one-table-one-field");
        assertNotNull(entity);
        assertEquals(5, entity.getFields().size());
        assertTrue(entity.getFields().containsKey("id"));
        assertTrue(entity.getFields().get("id").isPk());
        assertTrue(entity.getFields().containsKey("f1"));
        assertTrue(entity.getFields().containsKey("f2"));
        assertTrue(entity.getFields().containsKey("F3"));
        assertTrue(entity.getFields().containsKey("fieldb"));
    }

    @Test
    public void allColumnsWithSubEntity() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/all-columns/with-sub-entity");
        assertNotNull(entity);
        assertEquals(10, entity.getFields().size());
        assertTrue(entity.getFields().containsKey("id"));
        assertTrue(entity.getFields().get("id").isPk());
        assertTrue(entity.getFields().containsKey("f1"));
        assertTrue(entity.getFields().containsKey("F2"));
        assertTrue(entity.getFields().containsKey("f3"));
        assertTrue(entity.getFields().containsKey("fielda"));
        assertTrue(entity.getFields().containsKey("fieldb"));
        assertTrue(entity.getFields().containsKey("fieldc"));
        assertTrue(entity.getFields().containsKey("ORDER_NO"));
        assertTrue(entity.getFields().containsKey("amount"));
        assertTrue(entity.getFields().containsKey("customER"));
    }

    @Test
    public void allColumnsOneSubEntity() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/all-columns/one-sub-entity");
        assertNotNull(entity);
        assertEquals(3, entity.getFields().size());
        assertTrue(entity.getFields().containsKey("ORDER_NO"));
        assertTrue(entity.getFields().containsKey("amOUnt"));
        assertTrue(entity.getFields().containsKey("customER"));
    }

    @Test
    public void columnsWithoutSource() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/columns/without-source");
        assertNotNull(entity);
        assertEquals(2, entity.getFields().size());
        Field mdent_name = entity.getFields().get("mdent_name");
        assertEquals("MDENT_NAME", mdent_name.getName());
        assertEquals(DataTypes.STRING_TYPE_NAME, mdent_name.getType());
        assertEquals("MTD_ENTITIES", mdent_name.getTableName());
        assertFalse(mdent_name.isPk());
        Field f1 = entity.getFields().get("f1");
        assertEquals("F1", f1.getName());
        assertEquals(DataTypes.NUMBER_TYPE_NAME, f1.getType());
        assertEquals("TABLE1", f1.getTableName());
        assertFalse(f1.isPk());
    }

    @Test
    public void columnsWithAliases() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/columns/with-aliases");
        assertNotNull(entity);
        assertEquals(3, entity.getFields().size());
        assertTrue(entity.getFields().containsKey("id"));
        assertTrue(entity.getFields().containsKey("amt"));
        assertEquals(DataTypes.NUMBER_TYPE_NAME, entity.getFields().get("amt").getType());
        assertTrue(entity.getFields().containsKey("customer"));
    }

    @Test
    public void columnsWithoutAliases() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/columns/without-aliases");
        assertNotNull(entity);
        assertEquals(3, entity.getFields().size());
        assertTrue(entity.getFields().containsKey("ORDER_ID"));
        assertTrue(entity.getFields().containsKey("AMOUNT"));
        assertEquals(DataTypes.NUMBER_TYPE_NAME, entity.getFields().get("AMOUNT").getType());
        assertTrue(entity.getFields().containsKey("CUSTOMER_NAME"));
    }

    @Test
    public void columnsFromTablesWithAndWithoutAliases() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/columns/from-tables-with-and-without-aliases");
        assertNotNull(entity);
        assertEquals(3, entity.getFields().size());
        assertTrue(entity.getFields().containsKey("ORDER_ID"));
        assertTrue(entity.getFields().containsKey("AMOUNT"));
        assertEquals(DataTypes.NUMBER_TYPE_NAME, entity.getFields().get("AMOUNT").getType());
        assertTrue(entity.getFields().containsKey("CUSTOMER_NAME"));
    }

    @Test
    public void withSchemaInColumns() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/columns/with-schema-in-columns");
        assertEquals(4, entity.getFields().size());
        Field mdent_id = entity.getFields().get("mdent_id");
        assertEquals("MDENt_ID", mdent_id.getName());
        assertEquals(DataTypes.STRING_TYPE_NAME, mdent_id.getType());
        assertTrue(mdent_id.isPk());
        Field mdent_name = entity.getFields().get("mdent_name");
        assertEquals("MDENT_NAME", mdent_name.getName());
        assertEquals(DataTypes.STRING_TYPE_NAME, mdent_name.getType());
        assertFalse(mdent_name.isPk());
        Field mdent_type = entity.getFields().get("mdent_type");
        assertEquals("MDENT_TYPe", mdent_type.getName());
        assertEquals(DataTypes.NUMBER_TYPE_NAME, mdent_type.getType());
        assertFalse(mdent_type.isPk());
        Field mdent_order = entity.getFields().get("mdent_order");
        assertEquals("MDENT_ORDER", mdent_order.getName());
        assertEquals(DataTypes.NUMBER_TYPE_NAME, mdent_order.getType());
        assertFalse(mdent_order.isPk());
    }

    @Test
    public void expressionColumn() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/columns/expression-column");
        assertNotNull(entity);
        assertTrue(entity.getFields().containsKey("txt"));
        assertEquals(DataTypes.STRING_TYPE_NAME, entity.getFields().get("txt").getType());
    }

    @Test
    public void expressionColumnWithoutAlias() {
        Entities entities = new SqlEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/columns/expression-column-without-alias");
        assertNotNull(entity);
        assertEquals(5, entity.getFields().size());
    }

}
