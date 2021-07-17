allprojects {
    repositories {
        maven {
            url = uri("https://repo.osgeo.org/repository/release/")
        }
        mavenCentral()
        maven {
            url = uri("https://clojars.org/repo/")
        }
        maven {
            url = uri("https://maven.geotoolkit.org/")
        }
    }
}

subprojects {
    apply {
        plugin("java")
        plugin("jacoco")
    }

    configurations {
        get("implementation").isTransitive = false
        get("compileOnly").isTransitive = false
        get("testImplementation").isTransitive = false
    }

    repositories {
        mavenLocal()
    }

    tasks.withType(JavaCompile::class) {
        options.encoding = "utf-8"
    }

    dependencies {
        add("testImplementation", "junit:junit:4.11")
    }

    tasks["jacocoTestReport"].dependsOn("test")
    tasks["check"].dependsOn("jacocoTestReport")

    (extensions["java"] as JavaPluginExtension).also {
        it.withJavadocJar()
        it.withSourcesJar()
    }
}
