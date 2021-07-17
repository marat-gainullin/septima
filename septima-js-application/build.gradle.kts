val servletVersion = "3.1.0"
val mailVersion = "1.4.7"
val jaspicVersion = "1.1.1"
val webSocketVersion = "1.1"

dependencies {
    compileOnly("javax.servlet:javax.servlet-api:$servletVersion")
    compileOnly("javax.websocket:javax.websocket-api:$webSocketVersion")
    compileOnly("javax.mail:mail:$mailVersion")
    compileOnly("javax.security.auth.message:javax.security.auth.message-api:$jaspicVersion")

    implementation("com.fasterxml.jackson.core:jackson-core:2.9.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.9.2")
    implementation(project(":septima-js-model"))
    implementation(project(":septima-js-data"))

    testImplementation("javax.servlet:javax.servlet-api:$servletVersion")
    testImplementation("javax.websocket:javax.websocket-api:$webSocketVersion")
    testImplementation("javax.mail:mail:$mailVersion")
    testImplementation("com.h2database:h2:1.4.193")
    testImplementation("org.mockito:mockito-core:2.25.1")
}
