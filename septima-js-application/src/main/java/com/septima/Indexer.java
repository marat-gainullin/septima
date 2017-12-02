package com.septima;

import com.septima.cache.ScriptDocument;
import com.septima.cache.ScriptDocuments;
import com.septima.script.JsDoc;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author mg
 */
public class Indexer {

    public interface ScanCallback {

        void moduleScanned(String aModuleName, ScriptDocument.ModuleDocument aModule, File aFile);
    }

    protected Path sourcePath;
    protected Path apiPath;
    protected Map<String, File> id2Paths = new HashMap<>();
    protected WatchService service;
    protected ScanCallback scanCallback;
    protected boolean autoScan = true;
    protected ScriptDocuments scriptDocuments;

    public Indexer(Path aSourcePath, Path aApiPath, ScriptDocuments aScriptDocuments) throws Exception {
        this(aSourcePath, aApiPath, aScriptDocuments, true, null);
    }

    public Indexer(Path aSourcePath, Path aApiPath, ScriptDocuments aScriptDocuments, ScanCallback aScanCallback) throws Exception {
        this(aSourcePath, aApiPath, aScriptDocuments, true, aScanCallback);
    }

    public Indexer(Path aSourcePath, Path aApiPath, ScriptDocuments aScriptDocuments, boolean aAutoScan, ScanCallback aScanCallback) throws Exception {
        super();
        autoScan = aAutoScan;
        scriptDocuments = aScriptDocuments;
        sourcePath = aSourcePath;
        apiPath = aApiPath;
        scanCallback = aScanCallback;
        if (autoScan) {
            checkRootDirectory();
            scanSource();
        }
    }

    private void checkRootDirectory() throws IllegalArgumentException {
        File srcDirectory = sourcePath.toFile();
        if (!srcDirectory.exists() || !srcDirectory.isDirectory()) {
            throw new IllegalArgumentException(String.format("%s doesn't point to a directory.", sourcePath.toString()));
        }
    }

    public void watch() throws Exception {
        service = FileSystems.getDefault().newWatchService();
    }

    public void unwatch() throws Exception {
        assert service != null;
        service.close();
        service = null;
    }

    public void rescan() {
        id2Paths.clear();
        checkRootDirectory();
        scanSource();
    }

    private void scanSource() {
        try {
            Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {
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
            // Files.walkFileTree may fail due to some programs activity
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
            ScriptDocument scriptDoc = scriptDocuments.get(defaultModuleName, aFile);
            Set<Map.Entry<String, ScriptDocument.ModuleDocument>> modulesDocs = scriptDoc.getModules().entrySet();
            modulesDocs.forEach((Map.Entry<String, ScriptDocument.ModuleDocument> aModuleDocEntry) -> {
                id2Paths.put(aModuleDocEntry.getKey(), aFile);
                if (scanCallback != null) {
                    scanCallback.moduleScanned(aModuleDocEntry.getKey(), aModuleDocEntry.getValue(), aFile);
                }
            });
        } else if (aFile.getName().endsWith(SeptimaFiles.SQL_FILE_END)) {
            String fileContent = FileUtils.readString(aFile, SeptimaFiles.DEFAULT_ENCODING);
            String queryName = SeptimaFiles.getAnnotationValue(fileContent, JsDoc.Tag.NAME_TAG);
            if (queryName != null && !queryName.isEmpty()) {
                id2Paths.put(queryName, aFile);
            }
        }
    }

    @Override
    public String getDefaultModuleName(File aFile) {
        String defaultModuleName = sourcePath.relativize(Paths.get(aFile.toURI())).toString().replace(File.separator, "/");
        defaultModuleName = defaultModuleName.substring(0, defaultModuleName.length() - SeptimaFiles.JAVASCRIPT_FILE_END.length());
        return defaultModuleName;
    }

    public Path getAppPath() {
        return sourcePath;
    }

    /**
     * Resolves an application element name to a path of local file.
     *
     * @param aName
     * @return
     * @throws Exception
     */
    @Override
    public File nameToFile(String aName) throws Exception {
        if (aName != null) {
            File file = id2Paths.get(aName);
            if (file != null) {
                return file;
            } else {
                String filyName = aName.replace('/', File.separatorChar);
                Path appResource = sourcePath.resolve(filyName);
                if (appResource.toFile().exists()) {// plain resource relative 'app' directory
                    return appResource.toFile();
                } else {
                    Path appJsResource = sourcePath.resolve(filyName + SeptimaFiles.JAVASCRIPT_FILE_END);
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
