package com.septima.gradle;

import org.gradle.api.GradleException;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskAction;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.w3c.dom.Element;

public class TomcatConfigure extends Copy {

    private Map<String, Map<String, String>> dataSourcesSettings;

    private String webAppsAt;

    private String tomcatConfFrom;

    private String tomcatConfTo;

    private String context;

    private String realmDataSourceName;

    private String httpPort = "8080";

    private String logLevel = "INFO";

    public Map<String, Map<String, String>> getDataSourcesSettings() {
        return dataSourcesSettings;
    }

    public void setDataSourcesSettings(Map<String, Map<String, String>> dataSourcesSettings) {
        this.dataSourcesSettings = dataSourcesSettings;
    }

    public String getWebAppsAt() {
        return webAppsAt;
    }

    public void setWebAppsAt(String webAppsAt) {
        this.webAppsAt = webAppsAt;
    }

    public String getTomcatConfFrom() {
        return tomcatConfFrom;
    }

    public void setTomcatConfFrom(String tomcatConfFrom) {
        this.tomcatConfFrom = tomcatConfFrom;
        from(tomcatConfFrom);
        include("default-web.xml");
        rename("default-web.xml", "web.xml");
    }

    public String getTomcatConfTo() {
        return tomcatConfTo;
    }

    public void setTomcatConfTo(String tomcatConfTo) {
        this.tomcatConfTo = tomcatConfTo;
        into(tomcatConfTo);
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getRealmDataSourceName() {
        return realmDataSourceName;
    }

    public void setRealmDataSourceName(String realmDataSourceName) {
        this.realmDataSourceName = realmDataSourceName;
    }

    public String getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(String httpPort) {
        this.httpPort = httpPort != null ? httpPort : "8080";
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    private static <T> T also(T target, Consumer<T> action) {
        action.accept(target);
        return target;
    }

    private static Element getElement(Element anElement, String tagName, String path) {
        var children = anElement.getElementsByTagName(tagName);
        if (children.getLength() == 0)
            throw new GradleException("'" + tagName + "' tag is not found in server.xml or it is not on path: '" + path + "'");
        return (Element) children.item(0);
    }

    private static Properties asProps(File inputFile) throws IOException {
        var loaded = new Properties();
        if (inputFile.exists()) {
            try (var it = new BufferedInputStream(new FileInputStream(inputFile))) {
                loaded.load(it);
            }
        }
        return loaded;
    }

    @TaskAction
    public void configureTomcat() throws IOException, TransformerException, SAXException, ParserConfigurationException {
        if (tomcatConfFrom == null) {
            throw new GradleException("Missing 'tomcatConfFrom' option");
        }
        if (tomcatConfTo == null) {
            throw new GradleException("Missing 'tomcatConfTo' option");
        }
        if (webAppsAt == null) {
            throw new GradleException("Missing 'webAppsAt' option");
        }
        if (dataSourcesSettings == null) {
            throw new GradleException("Missing 'dataSourcesSettings' option");
        }
        if (context == null) {
            throw new GradleException("Missing 'context' option");
        }
        var dbFactory = DocumentBuilderFactory.newInstance();
        var dFactory = dbFactory.newDocumentBuilder();
        var conf = dFactory.parse(getProject().file(tomcatConfFrom + "/server.xml"));
        var service = getElement(conf.getDocumentElement(), "Service", "Server/Service");
        var connector = getElement(service, "Connector", "Server/Service/Connector");
        connector.setAttribute("port", "" + httpPort);

        var engine = getElement(service, "Engine", "Server/Service/Engine");
        var host = getElement(engine, "Host", "Server/Service/Engine/Host");

        var contexts = host.getElementsByTagName("Context");
        if (contexts.getLength() == 0)
            throw new GradleException("'Context' tag not found in server.xml or it is not on path: 'Server/Service/Engine/Host/Context'");
        for (int i = 0; i < contexts.getLength(); i++) {
            var context = (Element) contexts.item(i);
            var path = context.getAttribute("path");
            if (path != null && path.contains("${application.context}")) {
                context.setAttribute("path", "/" + path.replace("${application.context}", this.context));
            } else {
                context.setAttribute("path", "/" + this.context);
            }
            context.setAttribute("docBase", webAppsAt + "/" + this.context);
            AtomicBoolean realmAdded = new AtomicBoolean(false);
            dataSourcesSettings.forEach((dataSourceName, dataSourceSettings) -> {
                if (realmDataSourceName != null && realmDataSourceName.equals(dataSourceName)) {
                    context.appendChild(also(conf.createElement("Realm"), realm -> {
                        realm.setAttribute("className", "org.apache.catalina.realm.DataSourceRealm");
                        realm.setAttribute("dataSourceName", dataSourceName);
                        realm.setAttribute("localDataSource", "true");
                        realm.setAttribute("userNameCol", "userEmail");
                        realm.setAttribute("roleNameCol", "userGroup");
                        realm.setAttribute("userCredCol", "userDigest");
                        realm.setAttribute("userRoleTable", "appUsersGroups");
                        realm.setAttribute("userTable", "appUsers");
                        realm.appendChild(also(conf.createElement("CredentialHandler"), credentialHandler -> {
                            credentialHandler.setAttribute("className", "org.apache.catalina.realm.MessageDigestCredentialHandler");
                            credentialHandler.setAttribute("algorithm", "md5");
                            credentialHandler.setAttribute("encoding", "utf-8");
                            credentialHandler.setAttribute("iterations", "1");
                            credentialHandler.setAttribute("saltLength", "0");
                        }));
                    }));
                    realmAdded.set(true);
                }
                context.appendChild(also(conf.createElement("Resource"), resource -> {
                    resource.setAttribute("name", dataSourceName);
                    resource.setAttribute("url", dataSourceSettings.get("url"));
                    resource.setAttribute("username", dataSourceSettings.get("user"));
                    resource.setAttribute("driverClassName", dataSourceSettings.get("driverClass"));
                    resource.setAttribute("type", "javax.sql.DataSource");
                    if (dataSourceSettings.get("password") != null)
                        resource.setAttribute("password", dataSourceSettings.get("password"));
                    /*
                    if (dataSourceSettings.get("schema") != null)
                        resource.setAttribute("schema", dataSourceSettings.get("schema"));
                    if (dataSourceSettings.get("maxConnections") != null)
                        resource.setAttribute("maxActive", dataSourceSettings.get("maxConnections"));
                    if (dataSourceSettings.get("maxStatements") != null)
                        resource.setAttribute("maxStatements", dataSourceSettings.get("maxStatements"));
                    */
                }));
            });
            if (realmDataSourceName != null && !realmAdded.get()) {
                throw new GradleException("Security realm can't be added because 'realmDataSourceName' data source name is not in 'dataSourcesSettings'.");
            }
        }
        var transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.transform(new DOMSource(conf), new StreamResult(getProject().file(tomcatConfTo + "/server.xml")));

        var loggingProps = asProps(getProject().file(tomcatConfFrom + "/logging.properties"));
        loggingProps.setProperty(".level", logLevel);
        loggingProps.setProperty("org.apache.juli.AsyncFileHandler.level", logLevel);
        loggingProps.setProperty("java.util.logging.ConsoleHandler.level", logLevel);
        try (var it = new FileOutputStream(getProject().file(tomcatConfTo + "/logging.properties"))) {
            loggingProps.store(it, "Tomcat logging configuration. Septima profile.");
        }
    }
}
