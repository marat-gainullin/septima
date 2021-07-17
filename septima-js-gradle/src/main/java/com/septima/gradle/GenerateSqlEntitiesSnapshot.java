package com.septima.gradle;

import com.septima.generator.EntitiesSnapshots;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;

public class GenerateSqlEntitiesSnapshot extends GenerateTask {

    private EntitiesSnapshots generator;

    @InputDirectory
    private File sourceDir;

    @OutputDirectory
    private File targetDir;

    public EntitiesSnapshots getGenerator() {
        return generator;
    }

    public void setGenerator(EntitiesSnapshots generator) {
        this.generator = generator;
    }

    public File getSourceDir() {
        return sourceDir;
    }

    public void setSourceDir(File sourceDir) {
        this.sourceDir = sourceDir;
    }

    public File getTargetDir() {
        return targetDir;
    }

    public void setTargetDir(File targetDir) {
        this.targetDir = targetDir;
    }

    @TaskAction
    public void generate() throws IOException {
        if (sqlEntities == null) {
            throw new GradleException("'sqlEntities' property is required for" +
                    " the task");
        }
        if (generator == null) {
            generator = new EntitiesSnapshots(sqlEntities, targetDir.toPath());
        }

        System.out.println("Sql entities are read from '" + sqlEntities.getEntitiesRoot() + "'");
        System.out.println("Metadata snapshot is written to '" + targetDir + "'");

        Set<String> processed = new HashSet<>();
        Action<File> transform = sqlEntityFile -> {
            if (!processed.contains(sqlEntityFile.getAbsolutePath())) {
                try {
                    Path filledSnapshot = generator.toSnapshotJson(sqlEntityFile.toPath());
                    processed.add(sqlEntityFile.getAbsolutePath());
                    System.out.println("Metadata snapshot for '" + sqlEntities.getEntitiesRoot().relativize(sqlEntityFile.toPath()) + "' has been saved.");
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
        };

        Files.walkFileTree(sourceDir.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) {
                File file = filePath.toFile();
                if (!file.isDirectory()) {
                    if (file.getName().endsWith(".sql")) {
                        transform.execute(file);
                    } else if (file.getName().endsWith(".sql.json")) {
                        File origin = filePath.resolveSibling(file.getName().substring(0, file.getName().length() - 5)).toFile();
                        if (origin.exists()) {
                            transform.execute(origin);
                        } else {
                            throw new IllegalStateException("Additional info (*.sql.json) file without main (*.sql) file detected: " + filePath);
                        }
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
