package com.septima.gradle;

import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;
import com.septima.generator.ModelsDomains;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class GenerateDomains extends GenerateTask {

    private ModelsDomains generator;

    @InputDirectory
    private File sourceDir;

    @OutputDirectory
    private File targetDir;

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
            throw new GradleException("Property 'sqlEntities' is required for" +
                    " the task");
        }
        if (generator == null) {
            generator = ModelsDomains.fromResources(sqlEntities, sourceDir.toPath(), targetDir.toPath());
        }

        System.out.println("Models definitions are read from '" + sourceDir.toPath() + "'");
        System.out.println("Generated model classes are written to '" + targetDir.toPath() + "'");

        Path sourcePath = sourceDir.toPath();
        Path targetPath = targetDir.toPath();
        Files.walkFileTree(sourceDir.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) throws IOException {
                File file = filePath.toFile();
                if (!file.isDirectory()) {
                    if (file.getName().endsWith(".model.json")) {
                        Path generatedClass = generator.toJavaSource(filePath);
                        System.out.println("Model definition '" + sourcePath.relativize(filePath) + "' transformed to '" + targetPath.relativize(generatedClass) + "'");
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
