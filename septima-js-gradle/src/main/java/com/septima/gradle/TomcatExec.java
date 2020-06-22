package com.septima.gradle;

import org.gradle.api.GradleException;
import org.gradle.api.tasks.JavaExec;
import org.gradle.api.tasks.TaskAction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class TomcatExec extends JavaExec {

    private String initPath = "/init";
    private String tomcatAt;
    private String debugPort = "5007";
    private String applicationAt;

    public String getInitPath() {
        return initPath;
    }

    public void setInitPath(String initPath) {
        Objects.requireNonNull(initPath, "initPath is required");
        if (initPath.isBlank()) {
            throw new IllegalArgumentException("initPath is required");
        }
        this.initPath = initPath;
    }

    public String getTomcatAt() {
        return tomcatAt;
    }

    public void setTomcatAt(String tomcatAt) {
        this.tomcatAt = tomcatAt;
        setMain("org.apache.catalina.startup.Bootstrap");
        setWorkingDir(getProject().file(tomcatAt));
        classpath(getProject().fileTree(tomcatAt + "/bin"));
        updateJvmArgs();
    }

    private void updateJvmArgs() {
        setJvmArgs(List.of(
                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:" + debugPort,
                "-Djava.util.logging.config.file=" + getProject().file(tomcatAt) + "/conf/logging.properties",
                "-Dmail.mime.charset=utf-8"
        ));
    }

    public String getDebugPort() {
        return debugPort;
    }

    public void setDebugPort(String debugPort) {
        this.debugPort = debugPort;
        updateJvmArgs();
    }

    public String getApplicationAt() {
        return applicationAt;
    }

    public void setApplicationAt(String applicationAt) {
        this.applicationAt = applicationAt;
    }

    @TaskAction
    @Override
    public void exec() {
        if (tomcatAt == null || tomcatAt.isEmpty()) {
            throw new GradleException("Missing 'tomcatAt' option");
        }
        if (applicationAt == null || applicationAt.isEmpty()) {
            throw new GradleException("Missing 'applicationAt' option");
        }
        try {
            var instance = new ProcessBuilder(getCommandLine())
                    .directory(getWorkingDir())
                    .redirectErrorStream(true).start();

            try (var stdOut = new BufferedReader(new InputStreamReader(instance.getInputStream()))) {
                var tomcatNextLine = stdOut.readLine();
                while (tomcatNextLine != null && !tomcatNextLine.contains("Server startup in")) {
                    if (!tomcatNextLine.isEmpty())
                        System.out.println(tomcatNextLine);
                    tomcatNextLine = stdOut.readLine();
                }
                if (tomcatNextLine != null) {
                    System.out.println(tomcatNextLine);
                } else {
                    throw new GradleException("Failed to start Tomcat");
                }
                initContext();
                System.out.println();
                System.out.println("Tomcat started.");
                System.out.println("Further logs you can find at: " + tomcatAt + "/logs");
            }
            System.out.println("Application is now served at: " + applicationAt);
            System.out.println();
            System.out.println("To terminate press enter ...");
            System.in.read();
            instance.destroy();
            instance.waitFor();
            System.out.println();
            System.out.println("Tomcat has been shut down");
            System.out.println();
            System.out.println("See you next time :)");
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private void initContext() throws IOException, InterruptedException {
        URL applicationUrl = new URL(applicationAt);
        InetAddress address = InetAddress.getByName(applicationUrl.getHost());
        int port = applicationUrl.getPort() != -1 ? applicationUrl.getPort() : 80;
        String request = "GET " + applicationUrl.getPath() + initPath + " HTTP/1.1\r\n" +
                "Host: " + applicationUrl.getHost() + ":" + applicationUrl.getPort() + "\r\n" +
                "\r\n";
        try (Socket initRequestClient = new Socket(address, port)) {
            try (OutputStream out = initRequestClient.getOutputStream()) {
                out.write(request.getBytes(StandardCharsets.US_ASCII));
                Thread.sleep(2_000);
            }
        }
    }
}
