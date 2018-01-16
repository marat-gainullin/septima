package com.septima;

import org.h2.jdbcx.JdbcDataSource;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author mg
 */
public class TestDataSource {

    public static class Jndi implements InitialContextFactory {

        private static final Map<String, Object> binds = new ConcurrentHashMap<>();

        private class InitialMemoryContext extends InitialContext {

            public InitialMemoryContext(Hashtable<?, ?> env) throws NamingException {
            }

            @Override
            protected void init(Hashtable<?, ?> environment) throws NamingException {
                // no op here to avoid recursion with InitialContext
            }

            public Object lookup(String name) throws NamingException {
                if (!binds.containsKey(name)) {
                    throw new NamingException(name + " is not bound");
                }
                return binds.get(name);
            }

            public void bind(String name, Object obj) throws NamingException {
                binds.put(name, obj);
            }

            public void unbind(String name) throws NamingException {
                binds.remove(name);
            }

        }

        @Override
        public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
            return new InitialMemoryContext(environment);
        }
    }

    // Tests environment variables constants
    public static final String DATA_SOURCE_PROP_NAME = "datasource.name";
    private static final String DATA_SOURCE_URL_PROP = "datasource.url";
    private static final String DATA_SOURCE_USER_PROP = "datasource.user";
    private static final String DATA_SOURCE_PASSWORD_PROP = "datasource.password";
    private static final String DATA_SOURCE_SCHEMA_PROP = "datasource.schema";
    public static final String TEST_APP_PATH_PROP = "testsource.path";
    private static final String PROP_ERROR = " property is missing";

    public static void bind() throws NamingException {
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, Jndi.class.getName());

        String url = System.getProperty(DATA_SOURCE_URL_PROP);
        if (url == null) {
            throw new IllegalStateException(DATA_SOURCE_URL_PROP + TestDataSource.PROP_ERROR);
        }
        String user = System.getProperty(DATA_SOURCE_USER_PROP);
        if (user == null) {
            throw new IllegalStateException(DATA_SOURCE_USER_PROP + TestDataSource.PROP_ERROR);
        }
        String password = System.getProperty(DATA_SOURCE_PASSWORD_PROP);
        if (password == null) {
            throw new IllegalStateException(DATA_SOURCE_PASSWORD_PROP + TestDataSource.PROP_ERROR);
        }
        String schema = System.getProperty(DATA_SOURCE_SCHEMA_PROP);
        if (schema == null) {
            throw new IllegalStateException(DATA_SOURCE_SCHEMA_PROP + TestDataSource.PROP_ERROR);
        }

        JdbcDataSource source = new JdbcDataSource();
        source.setUrl(url);
        source.setUser(user);
        source.setPassword(password);
        Context ctx = new InitialContext();
        ctx.bind(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME), source);

        String appPath = System.getProperty(TEST_APP_PATH_PROP);
        if (appPath == null) {
            throw new IllegalStateException(TEST_APP_PATH_PROP + TestDataSource.PROP_ERROR);
        }
    }

    public static void unbind() throws NamingException {
        InitialContext ctx = new InitialContext();
        ctx.unbind(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME));
    }
}
