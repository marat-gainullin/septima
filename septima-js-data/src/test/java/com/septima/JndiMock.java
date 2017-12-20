package com.septima;

import javax.naming.*;
import javax.naming.spi.InitialContextFactory;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class JndiMock implements InitialContextFactory {

    private static final Map<String, Object> binds = new ConcurrentHashMap<>();

    private class InitialMemoryContext implements Context {

        private String nameToDots(Name name) {
            Iterator<String> names = name.getAll().asIterator();
            return Stream.generate(() -> names.hasNext() ? names.next() : null)
                    .map(s -> new StringBuilder())
                    .reduce((s1, s2) -> new StringBuilder().
                            append(s1).
                            append("/").
                            append(s2))
                    .toString();
        }

        private final Hashtable environment;

        public InitialMemoryContext(Hashtable<?, ?> env) {
            environment = env != null ? env : new Hashtable<>();
        }

        public Object lookup(Name name) throws NamingException {
            return binds.get(nameToDots(name));
        }

        public Object lookup(String name) throws NamingException {
            return binds.get(name);
        }

        public void bind(Name name, Object obj) throws NamingException {
            binds.put(nameToDots(name), obj);
        }

        public void bind(String name, Object obj) throws NamingException {
            binds.put(name, obj);
        }

        public void rebind(Name name, Object obj) throws NamingException {
            binds.put(nameToDots(name), obj);
        }

        public void rebind(String name, Object obj) throws NamingException {
            binds.put(name, obj);
        }

        public void unbind(Name name) throws NamingException {
            Iterator<String> names = name.getAll().asIterator();
            binds.remove(Stream.generate(() -> names.hasNext() ? names.next() : null)
                    .map(s -> new StringBuilder())
                    .reduce((s1, s2) -> new StringBuilder().append(s1).append("/").append(s2))
                    .toString());
        }

        public void unbind(String name) throws NamingException {
            binds.remove(name);
        }

        public void rename(Name oldName, Name newName) throws NamingException {
            throw new NamingException("Not supported");
        }

        public void rename(String oldName, String newName) throws NamingException {
            throw new NamingException("Not supported");
        }

        public NamingEnumeration<NameClassPair> list(Name name)
                throws NamingException {
            throw new NamingException("Not supported");
        }

        public NamingEnumeration<NameClassPair> list(String name)
                throws NamingException {
            throw new NamingException("Not supported");
        }

        public NamingEnumeration<Binding> listBindings(Name name)
                throws NamingException {
            throw new NamingException("Not supported");
        }

        public NamingEnumeration<Binding> listBindings(String name)
                throws NamingException {
            throw new NamingException("Not supported");
        }

        public void destroySubcontext(Name name) throws NamingException {
            throw new NamingException("Not supported");
        }

        public void destroySubcontext(String name) throws NamingException {
            throw new NamingException("Not supported");
        }

        public Context createSubcontext(Name name) throws NamingException {
            throw new NamingException("Not supported");
        }

        public Context createSubcontext(String name) throws NamingException {
            throw new NamingException("Not supported");
        }

        public Object lookupLink(Name name) throws NamingException {
            throw new NamingException("Not supported");
        }

        public Object lookupLink(String name) throws NamingException {
            throw new NamingException("Not supported");
        }

        public NameParser getNameParser(Name name) throws NamingException {
            throw new NamingException("Not supported");
        }

        public NameParser getNameParser(String name) throws NamingException {
            throw new NamingException("Not supported");
        }

        public Name composeName(Name name, Name prefix)
                throws NamingException {
            return ((Name) prefix.clone()).addAll(name);
        }

        public String composeName(String name, String prefix)
                throws NamingException {
            return prefix + name;
        }

        public Object addToEnvironment(String propName, Object propVal)
                throws NamingException {
            return environment.put(propName, propVal);
        }

        public Object removeFromEnvironment(String propName)
                throws NamingException {
            return environment.remove(propName);
        }

        public Hashtable<?, ?> getEnvironment() throws NamingException {
            return environment;
        }

        public void close() throws NamingException {
        }

        public String getNameInNamespace() throws NamingException {
            return "";
        }
    }

    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) {
        return new InitialMemoryContext(environment);
    }
}
