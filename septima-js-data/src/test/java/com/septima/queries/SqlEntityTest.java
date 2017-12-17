package com.septima.queries;

import com.septima.TestConstants;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * @author pk
 */
public class SqlEntityTest {

    private static final String PARAM2_VALUE = "qwerty";
    private static final String TWO_PARAMS_QUERY = "select * from ATABLE where FIELD1 > :param1 and FIELD2 = :param2 or FIELD1 < :param1";

    public SqlEntityTest() {
    }

    @BeforeClass
    public static void init() throws Exception {
        String url = System.getProperty(TestConstants.DATA_SOURCE_URL);
        if (url == null) {
            throw new IllegalStateException(TestConstants.DATA_SOURCE_URL + TestConstants.PROPERTY_ERROR);
        }
        String user = System.getProperty(TestConstants.DATA_SOURCE_USER);
        if (user == null) {
            throw new IllegalStateException(TestConstants.DATA_SOURCE_USER + TestConstants.PROPERTY_ERROR);
        }
        String passwd = System.getProperty(TestConstants.DATA_SOURCE_PASSWORD);
        if (passwd == null) {
            throw new IllegalStateException(TestConstants.DATA_SOURCE_PASSWORD + TestConstants.PROPERTY_ERROR);
        }
        String schema = System.getProperty(TestConstants.DATA_SOURCE_SCHEMA);
        if (schema == null) {
            throw new IllegalStateException(TestConstants.DATA_SOURCE_SCHEMA + TestConstants.PROPERTY_ERROR);
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }
/*
    @Test
    public void testCreation() {
        SqlEntity b.sql = new SqlEntity((DataSources) null);
        assertNull(b.sql.getSqlText());
        assertTrue(b.sql.getParametersBinds().isEmpty());
        b.sql.setSqlText(TWO_PARAMS_QUERY);
        assertEquals(b.sql.getSqlText(), TWO_PARAMS_QUERY);
        assertTrue(b.sql.getParametersBinds().isEmpty());
        b.sql.putParameter("param1", Scripts.NUMBER_TYPE_NAME, 1);
        b.sql.putParameter("param2", Scripts.STRING_TYPE_NAME, PARAM2_VALUE);
        assertEquals(2, b.sql.getParameters().getParametersCount());
    }

    @Test
    public void testCompiling() throws Exception {
        SqlEntity b.sql = new SqlEntity(resource.getClient());
        b.sql.setSqlText(TWO_PARAMS_QUERY);
        b.sql.putParameter("param1", Scripts.NUMBER_TYPE_NAME, 1);
        b.sql.putParameter("param2", Scripts.STRING_TYPE_NAME, PARAM2_VALUE);
        SqlCompiledQuery q = b.sql.compile();
        assertEquals(q.getSqlClause(), "select * from ATABLE where FIELD1 > ? and FIELD2 = ? or FIELD1 < ?");
        assertEquals(3, q.getParameters().getParametersCount());
        assertEquals(Scripts.NUMBER_TYPE_NAME, q.getParameters().get(1).getType());
        assertEquals(1, q.getParameters().get(1).getValue());
        assertEquals(Scripts.STRING_TYPE_NAME, q.getParameters().get(2).getType());
        assertEquals(PARAM2_VALUE, q.getParameters().get(2).getValue());
        assertEquals(Scripts.NUMBER_TYPE_NAME, q.getParameters().get(3).getType());
        assertEquals(1, q.getParameters().get(3).getValue());
    }
    */
}
