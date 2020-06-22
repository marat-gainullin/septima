package com.septima.application;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

public class Futures {
    private static final Map<String, Executor> executors = new ConcurrentHashMap<>();

    private static Executor lookupExecutor(String anExecutorName) {
        if (anExecutorName == null || anExecutorName.isBlank()) {
            throw new IllegalArgumentException("'executorName' is required");
        }
        return executors.computeIfAbsent(anExecutorName, Futures::obtainExecutor);
    }

    private static Executor obtainExecutor(String anExecutorName) {
        try {
            return (ExecutorService) InitialContext.doLookup("java:comp/env/" + anExecutorName);
        } catch (NamingException ex) {
            try {
                return (ExecutorService) InitialContext.doLookup("java:comp/" + anExecutorName);
            } catch (NamingException ex1) {
                try {
                    return lookupInTomcat(anExecutorName);
                } catch (NamingException ex2) {
                    return ForkJoinPool.commonPool();
                }
            }
        }
    }

    private static Executor lookupInTomcat(String anExecutorName) throws NamingException {
        Executor found;
        try {
            Object resources = InitialContext.doLookup("java:comp/Resources");
            Field contextField = resources.getClass().getDeclaredField("context");
            contextField.setAccessible(true);
            Object context = contextField.get(resources);
            Field innerContextField = context.getClass().getDeclaredField("context");
            innerContextField.setAccessible(true);
            Object innerContext = innerContextField.get(context);
            Field serviceField = innerContext.getClass().getDeclaredField("service");
            serviceField.setAccessible(true);
            Object service = serviceField.get(innerContext);
            Method getExecutor = service.getClass().getDeclaredMethod("getExecutor", String.class);
            found = (Executor) getExecutor.invoke(service, anExecutorName);
        } catch (Throwable th) {
            throw new NamingException("Couldn't lookup a Tomcat's executor");
        }
        if (found != null) {
            return found;
        } else {
            throw new NamingException("Name not found '" + anExecutorName + "'");
        }
    }

    public static void init(Config aConfig) {
        init(lookupExecutor(aConfig.getFuturesExecutorName()));
    }

    private static volatile Executor executor;

    public static void init(Executor anExecutor) {
        if (executor != null) {
            throw new IllegalStateException("Futures can be initialized only once.");
        }
        executor = anExecutor;
    }

    public static void done() {
        if (executor == null) {
            throw new IllegalStateException("Extra 'Futures' shutdown attempt detected.");
        }
        executor = null;
    }

    public static Executor getExecutor() {
        if (executor == null) {
            throw new IllegalStateException("An executor of futures is absent. Can't work without an executor");
        }
        return executor;
    }

}
