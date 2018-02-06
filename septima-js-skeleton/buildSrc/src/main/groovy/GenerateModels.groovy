import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.incremental.IncrementalTaskInputs
import com.septima.entities.SqlEntities
import com.septima.generator.Models

class GenerateModels extends DefaultTask {

    Models generator

    def SqlEntities sqlEntities

    @InputDirectory
    def File modelsDir

    @OutputDirectory
    def File generatedSourcesDir

    @TaskAction
    void generate(IncrementalTaskInputs inputs) {
        if (sqlEntities == null)
            throw new GradleException("Property 'sqlEntities' is required for" +
                    " the task")
        if (generator == null)
            generator = Models.fromResources(sqlEntities, modelsDir.toPath(), generatedSourcesDir.toPath())

        if (!inputs.incremental) {
            project.delete generatedSourcesDir.listFiles()
        }

        inputs.outOfDate { change ->
            if (change.file.name.endsWith(".model.json")) {
                def generatedClass = generator.toJavaSource(change.file.toPath())
                println "Model definition '${change.file}' transformed to '${generatedClass}'"
            }
        }

        inputs.removed { change ->
            if (change.file.name.endsWith(".model.json")) {
                def targetFile = generator.considerJavaSource(change.file.toPath()).toFile()
                if (targetFile.exists()) {
                    targetFile.delete()
                    println "Removed generated model class  : ${targetFile}"
                }
            }
        }
    }
}
