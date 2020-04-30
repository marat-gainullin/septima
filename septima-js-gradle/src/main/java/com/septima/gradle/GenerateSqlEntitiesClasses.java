package com.septima.gradle;

import com.septima.generator.EntitiesRaws;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;

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

public class GenerateSqlEntitiesClasses extends GenerateTask {

    private EntitiesRaws generator;

    @InputDirectory
    private File sourceDir;

    @OutputDirectory
    private File targetDir;

    public EntitiesRaws getGenerator() {
        return generator;
    }

    public void setGenerator(EntitiesRaws generator) {
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
            generator = EntitiesRaws.fromResources(sqlEntities, targetDir.toPath());
        }

        Set<String> processed = new HashSet<>();
        Action<File> transform = sqlEntityFile -> {
            if (!processed.contains(sqlEntityFile.getAbsolutePath())) {
                try {
                    Path generatedClass = generator.toJavaSource(sqlEntityFile.toPath());
                    processed.add(sqlEntityFile.getAbsolutePath());
                    System.out.println("Sql entity definition '" + sqlEntityFile + "' transformed to '" + generatedClass + "'");
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                } catch (IllegalStateException ex) {
                    System.out.println("Query '" + sqlEntityFile + "' has been skipped: '" + ex.getMessage() + "'");
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
