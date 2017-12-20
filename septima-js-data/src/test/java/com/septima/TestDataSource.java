package com.septima;

import org.h2.jdbcx.JdbcDataSource;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * @author mg
 */
public class TestDataSource {

    // Tests environment variables constants
    public static final String DATA_SOURCE_PROP_NAME = "datasource.name";
    private static final String DATA_SOURCE_URL_PROP = "datasource.url";
    private static final String DATA_SOURCE_USER_PROP = "datasource.user";
    private static final String DATA_SOURCE_PASSWORD_PROP = "datasource.password";
    private static final String DATA_SOURCE_SCHEMA_PROP = "datasource.schema";
    private static final String TEST_SOURCE_URL_PROP = "testsource.url";
    private static final String PROP_ERROR = " property is missing";

    public static void bind() throws NamingException {
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.septima.JndiMock");
        // System.setProperty(Context.URL_PKG_PREFIXES, "com.septima");
        String url = System.getProperty(TestDataSource.DATA_SOURCE_URL_PROP);
        if (url == null) {
            throw new IllegalStateException(TestDataSource.DATA_SOURCE_URL_PROP + TestDataSource.PROP_ERROR);
        }
        String user = System.getProperty(TestDataSource.DATA_SOURCE_USER_PROP);
        if (user == null) {
            throw new IllegalStateException(TestDataSource.DATA_SOURCE_USER_PROP + TestDataSource.PROP_ERROR);
        }
        String passwd = System.getProperty(TestDataSource.DATA_SOURCE_PASSWORD_PROP);
        if (passwd == null) {
            throw new IllegalStateException(TestDataSource.DATA_SOURCE_PASSWORD_PROP + TestDataSource.PROP_ERROR);
        }
        String schema = System.getProperty(TestDataSource.DATA_SOURCE_SCHEMA_PROP);
        if (schema == null) {
            throw new IllegalStateException(TestDataSource.DATA_SOURCE_SCHEMA_PROP + TestDataSource.PROP_ERROR);
        }
        JdbcDataSource source = new JdbcDataSource();
        source.setUrl(url);
        source.setUser(user);
        source.setPassword(passwd);
        Context ctx = new InitialContext();
        ctx.bind(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME), source);
    }

    public static void unbind() throws NamingException {
        InitialContext ctx = new InitialContext();
        ctx.unbind(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME));
    }
}
