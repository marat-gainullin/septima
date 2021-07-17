val h2 by configurations.creating

dependencies {
    implementation(project(":septima-js-sql-parser"))
    implementation("com.fasterxml.jackson.core:jackson-core:2.9.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.9.2")
    testImplementation("com.h2database:h2:1.4.193")
    testImplementation("com.vividsolutions:jts:1.13")

    h2("com.h2database:h2:1.4.193")
    h2("com.vividsolutions:jts:1.13")

    testRuntimeOnly(project(":septima-js-sql-driver-db2"))
    testRuntimeOnly(project(":septima-js-sql-driver-h2"))
    testRuntimeOnly(project(":septima-js-sql-driver-mssql"))
    testRuntimeOnly(project(":septima-js-sql-driver-mysql"))
    testRuntimeOnly(project(":septima-js-sql-driver-postgresql"))
    testRuntimeOnly(project(":septima-js-sql-driver-oracle"))
    testRuntimeOnly("com.oracle.database.jdbc:ojdbc10:19.11.0.0")
    testRuntimeOnly("org.postgresql:postgresql:42.1.4")
    testRuntimeOnly("net.postgis:postgis-jdbc:2.5.0")
    testRuntimeOnly("net.postgis:postgis-geometry:2.5.0")
    testRuntimeOnly("com.h2database:h2:1.4.193")
    testRuntimeOnly("com.vividsolutions:jts:1.13")
}

val appDirName = "${projectDir}/src/test/resources"
val h2Dir = "${buildDir}/h2"
val testBasesDirectory = "${buildDir}/test-databases"
val dataSourceName = "septima"
val dataSourceUrl = "jdbc:h2:/${testBasesDirectory}/septima"
val dataSourceUser = "sa"
val dataSourcePassword = "sa"
val dataSourceSchema = "PUBLIC"

val buildH2 = tasks.register("buildH2", Copy::class) {
    configurations["h2"].forEach {
        from(it)
    }
    into(h2Dir)
}

val fillTestBase = tasks.register("fillTestBase", JavaExec::class) {
    delete(testBasesDirectory)
    classpath(fileTree(h2Dir))
    main = "org.h2.tools.RunScript"
    args("-url", dataSourceUrl, "-user", dataSourceUser, "-password", dataSourcePassword, "-script", "${projectDir}/src/test/resources/septima.sql")
    doLast {
        println("Test database \"septima\" filled.")
    }
    dependsOn(buildH2)
}

val eraseTestBase = tasks.register("eraseTestBase", Delete::class) {
    delete(testBasesDirectory)
    doLast {
        println("Test database \"septima\" erased")
    }
    mustRunAfter(tasks["test"])
}

tasks.getByName("test", Test::class) {
    dependsOn(fillTestBase)
    finalizedBy(eraseTestBase)
    systemProperties["datasource.name"] = dataSourceName
    systemProperties["datasource.url"] = dataSourceUrl
    systemProperties["datasource.user"] = dataSourceUser
    systemProperties["datasource.password"] = dataSourcePassword
    systemProperties["datasource.schema"] = dataSourceSchema
    systemProperties["testsource.path"] = appDirName
}
