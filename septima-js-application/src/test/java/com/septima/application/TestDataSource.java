package com.septima.application;

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

    public static final String DATA_SOURCE_NAME = "septima-test-application-data";
    public static final String DATA_SOURCE_URL = "jdbc:h2:mem:septima-test-application";
    public static final String DATA_SOURCE_USER = "sa";
    public static final String DATA_SOURCE_PASSWORD = "sa";

    public static void bind() throws NamingException {
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, Jndi.class.getName());

        JdbcDataSource source = new JdbcDataSource();
        source.setUrl(DATA_SOURCE_URL);
        source.setUser(DATA_SOURCE_USER);
        source.setPassword(DATA_SOURCE_PASSWORD);
        Context ctx = new InitialContext();
        ctx.bind(DATA_SOURCE_NAME, source);
    }

    public static void unbind() throws NamingException {
        InitialContext ctx = new InitialContext();
        ctx.unbind(DATA_SOURCE_NAME);
    }

    public static class Jndi implements InitialContextFactory {

        private static final Map<String, Object> binds = new ConcurrentHashMap<>();

        @Override
        public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
            return new InitialMemoryContext(environment);
        }

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
    }
}
