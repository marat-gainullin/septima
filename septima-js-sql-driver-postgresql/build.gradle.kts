dependencies {
    compileOnly("org.postgresql:postgresql:42.1.4")
    compileOnly("net.postgis:postgis-jdbc:2.5.0")
    compileOnly("net.postgis:postgis-geometry:2.5.0")
    implementation(project(":septima-js-data"))
}
