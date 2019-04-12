package com.septima.gradle;

import com.septima.entities.SqlEntities;
import org.gradle.api.DefaultTask;

import java.io.File;

public class GenerateTask extends DefaultTask {

    protected SqlEntities sqlEntities;

    protected File entitiesRoot;

    protected String defaultDataSourceName;

    public String getDefaultDataSourceName() {
        return defaultDataSourceName;
    }

    public void setDefaultDataSourceName(String defaultDataSourceName) {
        this.defaultDataSourceName = defaultDataSourceName;
        checkSqlEntities();
    }

    public File getEntitiesRoot() {
        return entitiesRoot;
    }

    public void setEntitiesRoot(File entitiesRoot) {
        this.entitiesRoot = entitiesRoot;
        checkSqlEntities();
    }

    private void checkSqlEntities() {
        if (entitiesRoot != null && defaultDataSourceName != null) {
            sqlEntities = new SqlEntities(entitiesRoot.toPath(), defaultDataSourceName, true);
        }
    }
}
