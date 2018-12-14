package com.septima.gradle;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.incremental.IncrementalTaskInputs;
import com.septima.entities.SqlEntities;
import com.septima.generator.ModelsDomains;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

public class GenerateDomains extends GenerateTask {

    private ModelsDomains generator;

    @InputDirectory
    private File modelsDir;

    @OutputDirectory
    private File generatedSourcesDir;

    public File getModelsDir() {
        return modelsDir;
    }

    public void setModelsDir(File modelsDir) {
        this.modelsDir = modelsDir;
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
            throw new GradleException("Property 'sqlEntities' is required for" +
                    " the task");
        }
        if (generator == null)
            generator = ModelsDomains.fromResources(sqlEntities, modelsDir.toPath(), generatedSourcesDir.toPath());

        if (!inputs.isIncremental()) {
            getProject().delete(generatedSourcesDir.listFiles());
        }

        inputs.outOfDate(change -> {
            try {
                if (change.getFile().getName().endsWith(".model.json")) {
                    Path generatedClass = generator.toJavaSource(change.getFile().toPath());
                    System.out.println("Model definition '" + change.getFile() + "' transformed to '" + generatedClass + "'");
                }
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        });

        inputs.removed(change -> {
            if (change.getFile().getName().endsWith(".model.json")) {
                File targetFile = generator.considerJavaSource(change.getFile().toPath()).toFile();
                if (targetFile.exists()) {
                    targetFile.delete();
                    System.out.println("Removed generated model class: " + targetFile);
                }
            }
        });
    }
}
