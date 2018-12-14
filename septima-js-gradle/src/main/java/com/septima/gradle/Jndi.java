package com.septima.gradle;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Hashtable;
import java.util.Map;

public class Jndi implements InitialContextFactory {

    private static final Map<String, Object> binds = new ConcurrentHashMap<>();

    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
        return new InitialMemoryContext(environment);
    }

    private class InitialMemoryContext extends InitialContext {

        InitialMemoryContext(Hashtable<?, ?> env) throws NamingException {
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

    public static void bind(String name, Object target) throws NamingException {
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, Jndi.class.getName());
        InitialContext naming = new InitialContext();
        naming.bind(name, target);
    }
}

