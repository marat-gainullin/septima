import com.septima.entities.SqlEntities
import com.septima.generator.EntitiesRows
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.incremental.IncrementalTaskInputs

class GenerateEntities extends DefaultTask {

    def SqlEntities sqlEntities

    EntitiesRows generator

    @InputDirectory
    def File sqlEntitiesDir

    @OutputDirectory
    def File generatedSourcesDir

    @TaskAction
    void generate(IncrementalTaskInputs inputs) {
        if (sqlEntities == null)
            throw new GradleException("'sqlEntities' property is required for" +
                    " the task")
        if (generator == null)
            generator = EntitiesRows.fromResources(sqlEntities, generatedSourcesDir.toPath())

        if (!inputs.incremental) {
            project.delete generatedSourcesDir.listFiles()
        }

        def transform = { sqlEntity ->
            def generatedClass = generator.toJavaSource(sqlEntity.toPath())
            println "Sql entity definition '${sqlEntity}' transformed to '${generatedClass}'"
        }

        inputs.outOfDate { change ->
            if (change.file.name.endsWith(".sql")) {
                transform change.file
            } else if (change.file.name.endsWith(".sql.json")) {
                def origin = change.file.toPath().resolveSibling(change.file.name.substring(0, change.file.name.length() - 5)).toFile()
                if (origin.exists()) {
                    transform origin
                }
            }
        }

        def removeGeneratedClassBySqlFile = { file ->
            def targetFile = generator.considerJavaSource(file.toPath()).toFile()
            if (targetFile.exists()) {
                targetFile.delete()
                println "Removed generated entity class  : ${targetFile}"
            }
        }

        inputs.removed { change ->
            if (change.file.name.endsWith(".sql")) {
                removeGeneratedClassBySqlFile(change.file)
            } else if (change.file.name.endsWith(".sql.json")) {
                def origin = change.file.toPath().resolveSibling(change.file.name.substring(0, change.file.name.length() - 5)).toFile()
                if (origin.exists()) {
                    removeGeneratedClassBySqlFile(origin)
                }
            }
        }
    }
}
