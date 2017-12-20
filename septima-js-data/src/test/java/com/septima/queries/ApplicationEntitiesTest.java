package com.septima.queries;

import com.septima.TestDataSource;
import org.junit.*;

import javax.naming.NamingException;

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

    @Test
    public void threeLayerParameters(){
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
