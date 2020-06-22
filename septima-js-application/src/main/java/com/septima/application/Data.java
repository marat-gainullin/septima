package com.septima.application;

import com.septima.Database;
import com.septima.entities.SqlEntities;

public class Data {

    private static volatile Data instance;
    private final SqlEntities entities;

    private Data(SqlEntities aEntities) {
        entities = aEntities;
    }

    public static void init(Config aConfig) {
        if (instance != null) {
            throw new IllegalStateException("Data can be initialized only once.");
        }
        instance = new Data(new SqlEntities(
                aConfig.getResourcesEntitiesPath(),
                aConfig.getEntitiesPath(),
                aConfig.getDefaultDataSourceName(),
                Database.jdbcTasksPerformer(aConfig.getMaximumJdbcThreads()),
                Futures.getExecutor(),
                Boolean.getBoolean("com.septima.entities.compile")
        ));
    }

    public static void done() {
        if (instance == null) {
            throw new IllegalStateException("Extra data shutdown attempt detected.");
        }
        instance = null;
    }

    public static Data getInstance() {
        if (instance == null) {
            throw new IllegalStateException("The data infrastructure is not initialized.");
        }
        return instance;
    }

    public SqlEntities getEntities() {
        return entities;
    }
}
