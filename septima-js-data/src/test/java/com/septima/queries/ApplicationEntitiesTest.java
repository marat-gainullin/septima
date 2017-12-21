package com.septima.queries;

import com.septima.Entities;
import com.septima.Parameter;
import com.septima.TestDataSource;
import com.septima.application.ApplicationDataTypes;
import com.septima.application.ApplicationEntities;
import com.septima.metadata.Field;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import javax.naming.NamingException;
import java.io.File;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author mg
 */
public class ApplicationEntitiesTest {

    private static String rn2n(String withRn) {
        return withRn.replace("\r\n", "\n").replace("\n\r", "\n").replace("\r", "\n");
    }

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        TestDataSource.bind();
    }

    @Test(expected = IllegalStateException.class)
    public void badDataSource() {
        Entities entities = new ApplicationEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/bad-data-source");
    }

    /**
     * If sql text is unparsable, then no special action are needed.
     * User should provide parsable sql text and use customSql to get what he wants.
     */
    @Ignore
    @Test(expected = JSQLParserException.class)
    public void unparsableSqlText() {
        Entities entities = new ApplicationEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        entities.loadEntity("entities/unparsable");
    }

    @Test
    public void ethalonJsonContent() {
        Entities entities = new ApplicationEntities(
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
        assertEquals(ApplicationDataTypes.NUMBER_TYPE_NAME, quantity.getType());
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
        Entities entities = new ApplicationEntities(
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
        assertEquals(ApplicationDataTypes.STRING_TYPE_NAME, order_id.getType());
        assertNotNull(order_id.getFk());
    }

    @Test
    public void twoLevelInline() {
        Entities entities = new ApplicationEntities(
                new File(System.getProperty(TestDataSource.TEST_APP_PATH_PROP)).toPath(),
                System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME)
        );
        SqlEntity entity = entities.loadEntity("entities/inline/simple/a");
        assertNotNull(entity);
        assertEquals("", entity.getSqlText());
    }


/*
    @Test
    public void inlineSubQueries() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        SqlEntity testQuery = queriesProxy.getQuery("sub_query_compile", null, null, null);
        assertEquals(rn2n(""
                        + "SELECT T0.ORDER_NO, 'Some text' AS VALUE_FIELD_1, TABLE1.ID, TABLE1.F1, TABLE1.F3, T0.AMOUNT FROM TABLE1, TABLE2,\n"
                        + "(Select goodOrder.ORDER_ID as ORDER_NO, goodOrder.AMOUNT, customers.CUSTOMER_NAME as CUSTOMER \n"
                        + "From GOODORDER goodOrder\n"
                        + " Inner Join CUSTOMER customers on (goodOrder.CUSTOMER = customers.CUSTOMER_ID)\n"
                        + " and (goodOrder.AMOUNT > customers.CUSTOMER_NAME)\n"
                        + " Where :P4 = goodOrder.GOOD)  T0  WHERE ((TABLE2.FIELDA<TABLE1.F1) AND (:P2=TABLE1.F3)) AND (:P3=T0.AMOUNT)\n"),
                rn2n(testQuery.getSqlText()));
        assertEquals(6, testQuery.getFields().getFieldsCount());
        for (int i = 0; i < testQuery.getFields().getFieldsCount(); i++) {
            Field fieldMtd = testQuery.getFields().get(i + 1);
            assertNotNull(fieldMtd);
            // Jdbc driver indices oracle <= ojdbc6 doesn't support remarks for tables and for columns
//            if (i == 0 || i == 5) {
//                assertNotNull(fieldMtd.getDescription());
//            } else {
//                assertNull(fieldMtd.getDescription());
//            }
        }
        assertEquals(4, testQuery.getParameters().getParametersCount());
    }

    @Test
    public void inlineBadSubQueries() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        SqlEntity testQuery = queriesProxy.getQuery("bad_schema", null, null, null);
        assertEquals(rn2n(""
                        + "SELECT T0.ORDER_NO, 'Some text', TABLE1.ID, TABLE1.F1, TABLE1.F3, T0.AMOUNT FROM TABLE1, TABLE2,\n"
                        + "(Select goodOrder.ORDER_ID as ORDER_NO, goodOrder.AMOUNT, customers.CUSTOMER_NAME as CUSTOMER \n"
                        + "From GOODORDER goodOrder\n"
                        + " Inner Join CUSTOMER customers on (goodOrder.CUSTOMER = customers.CUSTOMER_ID)\n"
                        + " and (goodOrder.AMOUNT > customers.CUSTOMER_NAME)\n"
                        + " Where :P4 = goodOrder.GOOD)  T0  WHERE ((TABLE2.FIELDA<TABLE1.F1) AND (:P2=TABLE1.F3)) AND (:P3=T0.AMOUNT)\n"),
                rn2n(testQuery.getSqlText()));
        assertEquals(6, testQuery.getFields().getFieldsCount());
        for (int i = 0; i < testQuery.getFields().getFieldsCount(); i++) {
            Field fieldMtd = testQuery.getFields().get(i + 1);
            assertNotNull(fieldMtd);
            // Jdbc friver indices oracle <= ojdbc6 does not support remarks for tables and for columns
//            if (i == 0 || i == 5) {
//                assertNotNull(fieldMtd.getDescription());
//            } else {
//                assertNull(fieldMtd.getDescription());
//            }
        }
        assertEquals(4, testQuery.getParameters().getParametersCount());
    }

    @Test
    public void inlineAbsentSubQueries() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        SqlEntity testQuery = queriesProxy.getQuery("bad_schema", null, null, null);
        assertEquals(rn2n(""
                        + "SELECT T0.ORDER_NO, 'Some text', TABLE1.ID, TABLE1.F1, TABLE1.F3, T0.AMOUNT FROM TABLE1, TABLE2,\n"
                        + "(Select goodOrder.ORDER_ID as ORDER_NO, goodOrder.AMOUNT, customers.CUSTOMER_NAME as CUSTOMER \n"
                        + "From GOODORDER goodOrder\n"
                        + " Inner Join CUSTOMER customers on (goodOrder.CUSTOMER = customers.CUSTOMER_ID)\n"
                        + " and (goodOrder.AMOUNT > customers.CUSTOMER_NAME)\n"
                        + " Where :P4 = goodOrder.GOOD)  T0  WHERE ((TABLE2.FIELDA<TABLE1.F1) AND (:P2=TABLE1.F3)) AND (:P3=T0.AMOUNT)\n"),
                rn2n(testQuery.getSqlText()));
        assertEquals(6, testQuery.getFields().getFieldsCount());
        for (int i = 0; i < testQuery.getFields().getFieldsCount(); i++) {
            Field fieldMtd = testQuery.getFields().get(i + 1);
            assertNotNull(fieldMtd);
            // Jdbc friver indices oracle <= ojdbc6 does not support remarks for tables and for columns
//            if (i == 0 || i == 5) {
//                assertNotNull(fieldMtd.getDescription());
//            } else {
//                assertNull(fieldMtd.getDescription());
//            }
        }
        assertEquals(4, testQuery.getParameters().getParametersCount());
    }

    @Test(expected = IllegalStateException.class)
    public void badQueryName() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        queriesProxy.getQuery("bad_query_name", null, null, null);
    }

    @Test(expected = IllegalStateException.class)
    public void emptyQueryName() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        queriesProxy.getQuery("", null, null, null);
    }

    @Test
    public void allTablesFields() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        SqlEntity testQuery = queriesProxy.getQuery("asterisk_schema", null, null, null);
        assertEquals(rn2n(""
                        + "SELECT * FROM TABLE1, TABLE2,\n"
                        + "(Select goodOrder.ORDER_ID as ORDER_NO, goodOrder.AMOUNT, customers.CUSTOMER_NAME as CUSTOMER \n"
                        + "From GOODORDER goodOrder\n"
                        + " Inner Join CUSTOMER customers on (goodOrder.CUSTOMER = customers.CUSTOMER_ID)\n"
                        + " and (goodOrder.AMOUNT > customers.CUSTOMER_NAME)\n"
                        + " Where :P4 = goodOrder.GOOD)  T0  WHERE ((TABLE2.FIELDA<TABLE1.F1) AND (:P2=TABLE1.F3)) AND (:P3=T0.AMOUNT)"),
                rn2n(testQuery.getSqlText()));
        assertEquals(11, testQuery.getFields().getFieldsCount());
        for (int i = 0; i < testQuery.getFields().getFieldsCount(); i++) {
            Field fieldMtd = testQuery.getFields().get(i + 1);
            assertNotNull(fieldMtd);
        }
        assertEquals(4, testQuery.getParameters().getParametersCount());
    }

    @Test
    public void badSubQuery() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        SqlEntity testQuery = queriesProxy.getQuery("bad_subquery", null, null, null);
        assertEquals(rn2n(""
                + "SELECT * FROM TABLE1, TABLE2, #_1_2_8082898425059 T0 WHERE ((TABLE2.FIELDA<TABLE1.F1) AND (:P2=TABLE1.F3)) AND (:P3=T0.AMOUNT)\n"
                + ""), rn2n(testQuery.getSqlText()));
    }

    @Test
    public void allTableColumns() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        SqlEntity testQuery = queriesProxy.getQuery("partial_asterisk_schema", null, null, null);
        assertEquals(rn2n(""
                        + "SELECT TABLE1.*, TABLE2.FiELdB FROM TABLE1, TABLE2,\n"
                        + "(Select goodOrder.ORDER_ID as ORDER_NO, goodOrder.AMOUNT, customers.CUSTOMER_NAME as CUSTOMER \n"
                        + "From GOODORDER goodOrder\n"
                        + " Inner Join CUSTOMER customers on (goodOrder.CUSTOMER = customers.CUSTOMER_ID)\n"
                        + " and (goodOrder.AMOUNT > customers.CUSTOMER_NAME)\n"
                        + " Where :P4 = goodOrder.GOOD)  T0  WHERE ((TABLE2.FIELDA<TABLE1.F1) AND (:P2=TABLE1.F3)) AND (:P3=T0.AMOUNT)\n"),
                rn2n(testQuery.getSqlText()));
        assertEquals(5, testQuery.getFields().getFieldsCount());
        for (int i = 0; i < testQuery.getFields().getFieldsCount(); i++) {
            Field fieldMtd = testQuery.getFields().get(i + 1);
            assertNotNull(fieldMtd);
        }
        assertEquals(4, testQuery.getParameters().getParametersCount());
    }

    @Test
    public void primaryKey() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        SqlEntity testQuery = queriesProxy.getQuery("primary_key", null, null, null);
        Fields fields = testQuery.getFields();
        assertNotNull(fields);
        assertTrue(fields.getFieldsCount() > 0);
        assertTrue(fields.get(1).isPk());
    }

    @Test
    public void multiplePrimaryKeys() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        SqlEntity testQuery = queriesProxy.getQuery("multiple_primary_keys", null, null, null);
        Fields fields = testQuery.getFields();
        assertNotNull(fields);
        assertTrue(fields.getFieldsCount() == 2);
        assertTrue(fields.get(1).isPk());
        assertTrue(fields.get(2).isPk());
    }

    @Test
    public void withoutAliases_Schema_NonSchema_Schema_Columns() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        SqlEntity testQuery = queriesProxy.getQuery("without_aliases_with_schema_without_schema_columns_from_single_table", null, null, null);
        assertEquals(rn2n(""
                        + "SELECT EAS.MTD_EntitiES.MDENt_ID, MTD_EntitiES.MDENT_NAME, EAS.MTD_EntitiES.MDENT_TYPe, MDENT_ORDER FROM EaS.MTD_EntitiES\n"),
                rn2n(testQuery.getSqlText()));
        assertEquals(4, testQuery.getFields().getFieldsCount());
        for (int i = 0; i < testQuery.getFields().getFieldsCount(); i++) {
            Field fieldMtd = testQuery.getFields().get(i + 1);
            assertNotNull(fieldMtd);
        }
        assertEquals(0, testQuery.getParameters().getParametersCount());
    }

    @Test
    public void multiplePrimaryKeysWithAsterisk() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        SqlEntity testQuery = queriesProxy.getQuery("multiple_primary_keys_asterisk", null, null, null);
        Fields fields = testQuery.getFields();
        assertNotNull(fields);
        assertTrue(fields.getFieldsCount() == 23);
        assertNotNull(fields.get("MdENT_ID"));
        assertTrue(fields.get("MDENT_iD").isPk());
        assertNotNull(fields.get("MDlOG_ID"));
        assertTrue(fields.get("MDLOG_ID").isPk());
        assertFalse(fields.getPrimaryKeys().isEmpty());
        assertEquals(2, fields.getPrimaryKeys().size());
        assertEquals("mdent_id", fields.getPrimaryKeys().get(0).getName());
        assertEquals("mdlog_id", fields.getPrimaryKeys().get(1).getName());
    }

    @Test
    public void loadEntity() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        SqlEntity testQuery = queriesProxy.getQuery("get_query", null, null, null);
        Fields metadata = testQuery.getFields();
        assertEquals(3, metadata.getFieldsCount());
    }

    @Test
    public void loadEmptyEntity() throws Exception {
        LocalQueriesProxy queriesProxy = new LocalQueriesProxy(resource.getClient(), indexer);
        try {
            SqlEntity testQuery = queriesProxy.getQuery("empty_query", null, null, null);
            fail("Empty query must lead to an exception, but it doesn't. Why?");
        } catch (Exception ex) {
            //fine. there must be an exception
        }
    }
    */
}
