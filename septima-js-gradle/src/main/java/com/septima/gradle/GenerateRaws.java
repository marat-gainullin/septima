package com.septima.gradle;

import com.septima.entities.SqlEntities;
import com.septima.generator.EntitiesRaws;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.incremental.IncrementalTaskInputs;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

public class GenerateRaws extends GenerateTask {

    private EntitiesRaws generator;

    @InputDirectory
    private File sqlEntitiesDir;

    @OutputDirectory
    private File generatedSourcesDir;

    public EntitiesRaws getGenerator() {
        return generator;
    }

    public void setGenerator(EntitiesRaws generator) {
        this.generator = generator;
    }

    public File getSqlEntitiesDir() {
        return sqlEntitiesDir;
    }

    public void setSqlEntitiesDir(File sqlEntitiesDir) {
        this.sqlEntitiesDir = sqlEntitiesDir;
    }

    public File getGeneratedSourcesDir() {
        return generatedSourcesDir;
    }

    public void setGeneratedSourcesDir(File generatedSourcesDir) {
        this.generatedSourcesDir = generatedSourcesDir;
    }

    @TaskAction
    public void generate(IncrementalTaskInputs inputs) throws IOException {
        if (sqlEntities == null) {
            throw new GradleException("'sqlEntities' property is required for" +
                    " the task");
        }
        if (generator == null) {
            generator = EntitiesRaws.fromResources(sqlEntities, generatedSourcesDir.toPath());
        }

        if (!inputs.isIncremental()) {
            getProject().delete(generatedSourcesDir.listFiles());
        }

        Action<File> transform = sqlEntity -> {
            try {
                Path generatedClass = generator.toJavaSource(sqlEntity.toPath());
                System.out.println("Sql entity definition '" + sqlEntity + "' transformed to '" + generatedClass + "'");
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        };

        inputs.outOfDate(change -> {
            if (change.getFile().getName().endsWith(".sql")) {
                transform.execute(change.getFile());
            } else if (change.getFile().getName().endsWith(".sql.json")) {
                File origin = change.getFile().toPath().resolveSibling(change.getFile().getName().substring(0, change.getFile().getName().length() - 5)).toFile();
                if (origin.exists()) {
                    transform.execute(origin);
                }
            }
        });

        Action<File> removeGeneratedClassBySqlFile = file -> {
            File targetFile = generator.considerJavaSource(file.toPath()).toFile();
            if (targetFile.exists()) {
                targetFile.delete();
                System.out.println("Removed generated entity class: " + targetFile);
            }
        };

        inputs.removed(change -> {
            if (change.getFile().getName().endsWith(".sql")) {
                removeGeneratedClassBySqlFile.execute(change.getFile());
            } else if (change.getFile().getName().endsWith(".sql.json")) {
                File origin = change.getFile().toPath().resolveSibling(change.getFile().getName().substring(0, change.getFile().getName().length() - 5)).toFile();
                if (origin.exists()) {
                    removeGeneratedClassBySqlFile.execute(origin);
                }
            }
        });
    }
}
