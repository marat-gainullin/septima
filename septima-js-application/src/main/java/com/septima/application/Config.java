package com.septima.application;

import javax.servlet.ServletContext;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;

/**
 * Servlet configuration parser.
 *
 * @author mg
 */
public class Config {

    private static final String DATA_SOURCE_DEFAULT_CONF_PARAM = "data.source";
    private static final String DATA_BATCHES_CONF_PARAM = "data.batches";
    private static final String FUTURES_EXECUTOR_CONF_PARAM = "futures.executor";
    private static final String MAIL_SESSION_CONF_PARAM = "mail.session";
    private static final String MAX_JDBC_THREADS_CONF_PARAM = "jdbc.max.threads";
    private static final String MAX_BATCH_SIZE_CONF_PARAM = "max.batch.size";
    private static final String MAX_MAIL_THREADS_CONF_PARAM = "mail.max.threads";
    private static final String LPC_QUEUE_SIZE_CONF_PARAM = "scope.queue.size";
    private static final String ENTITIES_PATH_CONF_PARAM = "entities.path";
    private static final String RESOURCES_ENTITIES_PATH_CONF_PARAM = "resources.entities.path";

    private final String defaultDataSourceName;
    private final String futuresExecutorName;
    private final boolean dataBatches;
    private final int maximumBatchSize;
    private final int maximumJdbcThreads;
    private final int maximumMailThreads;
    private final int maximumLpcQueueSize;
    private final Path resourcesEntitiesPath;
    private final Path entitiesPath;

    private Config(String aDefaultDataSourceName, String aFuturesExecutorName, Path anEntitiesResourcesPath, Path anEntitiesPath, boolean aDataBatches, int aMaximumBatchSize, int aMaximumJdbcThreads, int aMaximumMailTreads, int aMaximumLpcQueueSize) {
        defaultDataSourceName = aDefaultDataSourceName;
        futuresExecutorName = aFuturesExecutorName;
        resourcesEntitiesPath = anEntitiesResourcesPath;
        entitiesPath = anEntitiesPath;
        dataBatches = aDataBatches;
        maximumBatchSize = aMaximumBatchSize;
        maximumJdbcThreads = aMaximumJdbcThreads;
        maximumMailThreads = aMaximumMailTreads;
        maximumLpcQueueSize = aMaximumLpcQueueSize;
    }

    public static Config parse(ServletContext aContext) {
        String defaultDataSourceName = null;
        String futuresExecutorName = null;
        String mailSessionName = null;
        boolean dataBatches = true;
        int maximumBatchSize = 128;
        int maximumJdbcThreads = 16;
        int maximumMailTreads = 16;
        int maximumLpcQueueSize = 1024;
        Path entitiesPath = null;
        Path entitiesResourcesPath = null;
        Enumeration<String> paramNames = aContext.getInitParameterNames();
        if (paramNames != null) {
            while (paramNames.hasMoreElements()) {
                String paramName = paramNames.nextElement();
                if (paramName != null) {
                    String paramValue = aContext.getInitParameter(paramName);
                    if (MAX_JDBC_THREADS_CONF_PARAM.equals(paramName)) {
                        maximumJdbcThreads = Math.max(1, Integer.parseInt(paramValue));
                    } else if (MAX_BATCH_SIZE_CONF_PARAM.equals(paramName)) {
                        maximumBatchSize = Math.max(1, Integer.parseInt(paramValue));
                    } else if (DATA_BATCHES_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        dataBatches = Boolean.parseBoolean(paramValue);
                    } else if (MAX_MAIL_THREADS_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        maximumMailTreads = Math.max(1, Integer.parseInt(paramValue));
                    } else if (LPC_QUEUE_SIZE_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        maximumLpcQueueSize = Math.max(1, Integer.parseInt(paramValue));
                    } else if (ENTITIES_PATH_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        entitiesPath = Paths.get(aContext.getRealPath(paramValue));
                    } else if (RESOURCES_ENTITIES_PATH_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        entitiesResourcesPath = Paths.get(paramValue.startsWith("/") ? paramValue : "/" + paramValue);
                    } else if (DATA_SOURCE_DEFAULT_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        defaultDataSourceName = paramValue;
                    } else if (FUTURES_EXECUTOR_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        futuresExecutorName = paramValue;
                    } else if (MAIL_SESSION_CONF_PARAM.equalsIgnoreCase(paramName)) {
                        mailSessionName = paramValue;
                    }
                }
            }
        }
        if (defaultDataSourceName != null && !defaultDataSourceName.isEmpty()) {
            if (entitiesResourcesPath != null ^ entitiesPath != null) {
                return new Config(defaultDataSourceName,
                        futuresExecutorName,
                        entitiesResourcesPath,
                        entitiesPath,
                        dataBatches,
                        maximumBatchSize,
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
            throw new IllegalStateException("Default data source has to be specified");
        }
    }

    public String getDefaultDataSourceName() {
        return defaultDataSourceName;
    }

    public String getFuturesExecutorName() {
        return futuresExecutorName;
    }

    public boolean isDataBatches() {
        return dataBatches;
    }

    public int getMaximumBatchSize() {
        return maximumBatchSize;
    }

    public int getMaximumJdbcThreads() {
        return maximumJdbcThreads;
    }

    public int getMaximumMailThreads() {
        return maximumMailThreads;
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
