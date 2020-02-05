package com.septima.gradle;

import com.septima.generator.EntitiesSnapshots;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.incremental.IncrementalTaskInputs;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

public class GenerateMetadata extends GenerateTask {

    private EntitiesSnapshots generator;

    @InputDirectory
    private File sqlEntitiesDir;

    @OutputDirectory
    private File targetDir;

    public EntitiesSnapshots getGenerator() {
        return generator;
    }

    public void setGenerator(EntitiesSnapshots generator) {
        this.generator = generator;
    }

    public File getSqlEntitiesDir() {
        return sqlEntitiesDir;
    }

    public void setSqlEntitiesDir(File sqlEntitiesDir) {
        this.sqlEntitiesDir = sqlEntitiesDir;
    }

    public File getTargetDir() {
        return targetDir;
    }

    public void setTargetDir(File targetDir) {
        this.targetDir = targetDir;
    }

    @TaskAction
    public void generate(IncrementalTaskInputs inputs) throws IOException {
        if (sqlEntities == null) {
            throw new GradleException("'sqlEntities' property is required for" +
                    " the task");
        }
        if (generator == null) {
            generator = new EntitiesSnapshots(sqlEntities, targetDir.toPath());
        }

        if (!inputs.isIncremental()) {
            getProject().delete(targetDir.listFiles());
        }

        Action<File> transform = sqlEntity -> {
            try {
                Path filledSnapshot = generator.toSnapshotJson(sqlEntity.toPath());
                System.out.println("Sql entity definition '" + sqlEntity + "' filled with metadata and saved to '" + filledSnapshot + "'");
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
            File targetFile = generator.considerSnapshotJson(file.toPath()).toFile();
            if (targetFile.exists()) {
                targetFile.delete();
                System.out.println("Removed metadata rich entity definition: " + targetFile);
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
