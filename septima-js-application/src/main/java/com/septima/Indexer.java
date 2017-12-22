package com.septima;

import com.septima.indexer.ScriptDocument;
import com.septima.script.JsDoc;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author mg
 */
public class Indexer {

    @FunctionalInterface
    public interface OnModule {

        void scanned(String aModuleName, ScriptDocument.ModuleDocument aModule, File aFile);
    }

    protected Path appPath;
    protected Path apiPath; // TODO: Remove after libraries / modules refactoring
    protected Map<String, File> fileByPath = new HashMap<>();
    protected OnModule onModule;

    public Indexer(Path aSourcePath, Path aApiPath) throws Exception {
        this(aSourcePath, aApiPath, null);
    }

    public Indexer(Path aSourcePath, Path aApiPath, OnModule aOnModule) throws Exception {
        super();
        appPath = aSourcePath;
        apiPath = aApiPath;
        onModule = aOnModule;
    }

    private void checkRootDirectory() throws IllegalArgumentException {
        File srcDirectory = appPath.toFile();
        if (!srcDirectory.exists() || !srcDirectory.isDirectory()) {
            throw new IllegalArgumentException(String.format("%s doesn't point transform a directory.", appPath.toString()));
        }
    }

    public void rescan() {
        fileByPath.clear();
        scanSource();
    }

    private void scanSource() {
        checkRootDirectory();
        try {
            Files.walkFileTree(appPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path aFilePath, BasicFileAttributes attrs) throws IOException {
                    if (apiPath != null && aFilePath.startsWith(apiPath)) {
                        return FileVisitResult.SKIP_SUBTREE;
                    } else {
                        try {
                            add(aFilePath.toFile());
                        } catch (Exception ex) {
                            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                }
            });
        } catch (Exception ex) {
            // Files.walkFileTree may fail due transform some programs activity
            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static String fileNameWithoutExtension(File aFile) {
        String filePath = aFile.getPath();
        int dotIndex = filePath.lastIndexOf('.');
        if (!filePath.isEmpty() && dotIndex != -1) {
            return filePath.substring(0, dotIndex);
        } else {
            return null;
        }
    }

    protected void add(File aFile) throws Exception {
        if (aFile.getName().endsWith(SeptimaFiles.JAVASCRIPT_FILE_END)) {
            String defaultModuleName = getDefaultModuleName(aFile);
            String source = new String(Files.readAllBytes(aFile.toPath()), StandardCharsets.UTF_8);
            ScriptDocument scriptDoc = ScriptDocument.parse(source, defaultModuleName);
            scriptDoc.getModules().entrySet()
                    .forEach((Map.Entry<String, ScriptDocument.ModuleDocument> aModuleDocEntry) -> {
                        fileByPath.put(aModuleDocEntry.getKey(), aFile);
                        if (onModule != null) {
                            onModule.scanned(aModuleDocEntry.getKey(), aModuleDocEntry.getValue(), aFile);
                        }
                    });
        } else if (aFile.getName().endsWith(SeptimaFiles.SQL_FILE_END)) {
            String fileContent = new String(Files.readAllBytes(aFile.toPath()), StandardCharsets.UTF_8);
            String queryName = SeptimaFiles.getAnnotationValue(fileContent, JsDoc.Tag.NAME_TAG);
            if (queryName != null && !queryName.isEmpty()) {
                fileByPath.put(queryName, aFile);
            }
        }
    }

    public String getDefaultModuleName(File aFile) {
        String defaultModuleName = appPath.relativize(Paths.get(aFile.toURI())).toString().replace(File.separator, "/");
        defaultModuleName = defaultModuleName.substring(0, defaultModuleName.length() - SeptimaFiles.JAVASCRIPT_FILE_END.length());
        return defaultModuleName;
    }

    public Path getAppPath() {
        return appPath;
    }

    /**
     * Resolves an application element name transform a path of local file.
     *
     * @param aName
     * @return
     * @throws Exception
     */
    public File nameToFile(String aName) throws Exception {
        if (aName != null) {
            File file = fileByPath.get(aName);
            if (file != null) {
                return file;
            } else {
                String filyName = aName.replace('/', File.separatorChar);
                Path appResource = appPath.resolve(filyName);
                if (appResource.toFile().exists()) {// plain resource relative 'app' directory
                    return appResource.toFile();
                } else {
                    Path appJsResource = appPath.resolve(filyName + SeptimaFiles.JAVASCRIPT_FILE_END);
                    if (appJsResource.toFile().exists()) {// *.js resource relative 'app' directory
                        return appJsResource.toFile();
                    } else {
                        File absoluteResource = new File(filyName);// plain resource by absolute path
                        if (absoluteResource.exists()) {
                            return absoluteResource;
                        } else {
                            File absoluteJsResource = new File(filyName + SeptimaFiles.JAVASCRIPT_FILE_END);
                            if (absoluteJsResource.exists()) {
                                return absoluteJsResource;
                            } else {
                                return null;
                            }
                        }
                    }
                }
            }
        } else {
            return null;
        }
    }
}
