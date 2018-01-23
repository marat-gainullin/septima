package com.septima.application;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletContext;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * Servlet configuration parser.
 *
 * @author mg
 */
public class Config {

    private static final String DEF_DATA_SOURCE_CONF_PARAM = "data-source";
    private static final String MAX_JDBC_THREADS_CONF_PARAM = "jdbc.max.threads";
    private static final String MAX_MAIL_THREADS_CONF_PARAM = "mail.max.threads";
    private static final String LPC_QUEUE_SIZE_CONF_PARAM = "scope.queue.size";
    private static final String ENTITIES_PATH_CONF_PARAM = "entities.path";
    private static final String RESOURCES_ENTITIES_PATH_CONF_PARAM = "resources.entities.path";

    private final String defaultDataSourceName;
    private final int maximumJdbcThreads;
    private final int maximumMailTreads;
    private final int maximumLpcQueueSize;
    private final Path resourcesEntitiesPath;
    private final Path entitiesPath;

    private Config(String aDefaultDataSourceName, Path anEntitiesResourcesPath, Path anEntitiesPath, int aMaximumJdbcThreads, int aMaximumMailTreads, int aMaximumLpcQueueSize) {
        defaultDataSourceName = aDefaultDataSourceName;
        resourcesEntitiesPath = anEntitiesResourcesPath;
        entitiesPath = anEntitiesPath;
        maximumJdbcThreads = aMaximumJdbcThreads;
        maximumMailTreads = aMaximumMailTreads;
        maximumLpcQueueSize = aMaximumLpcQueueSize;
    }

    public static Config parse(ServletContext aContext) {
        String defaultDataSourceName = null;
        int maximumJdbcThreads = 16;
        int maximumMailTreads = 16;
        int maximumLpcQueueSize = 1024;
        Path entitiesPath = null;
        Path entitiesResourcesPath = null;
        Enumeration<String> paramNames = aContext.getInitParameterNames();
        if (paramNames != null && paramNames.hasMoreElements()) {
            while (paramNames.hasMoreElements()) {
                String paramName = paramNames.nextElement();
                if (paramName != null) {
                    String paramValue = aContext.getInitParameter(paramName);
                    if (MAX_JDBC_THREADS_CONF_PARAM.equals(paramName)) {
                        maximumJdbcThreads = Math.max(1, Double.valueOf(paramValue).intValue());
                    } else if (MAX_MAIL_THREADS_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        maximumMailTreads = Math.max(1, Double.valueOf(paramValue).intValue());
                    } else if (LPC_QUEUE_SIZE_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        maximumLpcQueueSize = Math.max(1, Double.valueOf(paramValue).intValue());
                    } else if (ENTITIES_PATH_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        entitiesPath = Paths.get(aContext.getRealPath(paramValue));
                    } else if (RESOURCES_ENTITIES_PATH_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        entitiesResourcesPath = Paths.get(paramValue.startsWith("/") ? paramValue : "/" + paramValue);
                    } else if (DEF_DATA_SOURCE_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        defaultDataSourceName = paramValue;
                    }
                }
            }
        }
        if (defaultDataSourceName != null && !defaultDataSourceName.isEmpty()) {
            if (entitiesResourcesPath != null ^ entitiesPath != null) {
                return new Config(defaultDataSourceName,
                        entitiesResourcesPath,
                        entitiesPath,
                        maximumJdbcThreads,
                        maximumMailTreads,
                        maximumLpcQueueSize
                );
            } else if (entitiesResourcesPath != null) {
                throw new IllegalStateException("Only one of ['" + RESOURCES_ENTITIES_PATH_CONF_PARAM + "', '" + ENTITIES_PATH_CONF_PARAM + "'] parameters should to be specified");
            } else {
                throw new IllegalStateException("One of ['" + RESOURCES_ENTITIES_PATH_CONF_PARAM + "', '" + ENTITIES_PATH_CONF_PARAM + "'] parameters should to be specified");
            }
        } else {
            throw new IllegalStateException("Default data source have to be specified");
        }
    }

    public static ExecutorService lookupExecutor() {
        try {
            return (ExecutorService) InitialContext.doLookup("java:comp/DefaultManagedExecutorService");
        } catch (NamingException ex) {
            try {
                return (ExecutorService) InitialContext.doLookup("java:comp/env/concurrent/ThreadPool");
            } catch (NamingException ex1) {
                return ForkJoinPool.commonPool();
            }
        }
    }

    public String getDefaultDataSourceName() {
        return defaultDataSourceName;
    }

    public int getMaximumJdbcThreads() {
        return maximumJdbcThreads;
    }

    public int getMaximumMailTreads() {
        return maximumMailTreads;
    }

    public int getMaximumLpcQueueSize() {
        return maximumLpcQueueSize;
    }

    public Path getResourcesEntitiesPath() {
        return resourcesEntitiesPath;
    }

    public Path getEntitiesPath() {
        return entitiesPath;
    }
}
