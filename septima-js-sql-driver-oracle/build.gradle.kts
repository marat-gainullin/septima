dependencies {
    compileOnly("com.oracle.database.jdbc:ojdbc10:19.11.0.0")
    implementation(project(":septima-js-data"))
    implementation("org.geotools.jdbc:gt-jdbc-oracle:18.1")
    implementation("com.vividsolutions:jts:1.13")
}
