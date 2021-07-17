package com.septima.gradle;

import com.septima.entities.SqlEntities;
import org.gradle.api.DefaultTask;

import java.io.File;

public class GenerateTask extends DefaultTask {

    private File entitiesRoot;
    private String defaultDataSourceName;
    protected SqlEntities sqlEntities;

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

    public SqlEntities getSqlEntities() {
        return sqlEntities;
    }

    public void setSqlEntities(SqlEntities sqlEntities) {
        this.sqlEntities = sqlEntities;
    }

    private void checkSqlEntities() {
        if (entitiesRoot != null && defaultDataSourceName != null) {
            sqlEntities = new SqlEntities(entitiesRoot.toPath(), defaultDataSourceName, true, false, 1);
        }
    }

    public static SqlEntities sqlEntitiesOf(File entitiesRoot, String defaultDataSourceName) {
        return new SqlEntities(entitiesRoot.toPath(), defaultDataSourceName, true, false, 1);
    }
}
