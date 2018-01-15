package com.septima;

import com.septima.indexer.ScriptDocument;
import com.septima.script.JsDoc;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author mg
 */
public class ResidentModules implements Indexer.OnModule {

    private final Set<String> residents = new HashSet<>();

    public ResidentModules() {
        super();
    }

    @Override
    public void scanned(final String aModuleName, final ScriptDocument.ModuleDocument aModuleDocument, final File aFile) {
        List<JsDoc.Tag> annotations = aModuleDocument.getAnnotations();
        if (annotations.stream()
                .anyMatch(tag -> JsDoc.Tag.RESIDENT_TAG.equalsIgnoreCase(tag.getName()))
                ) {
            residents.add(aModuleName);
        }
    }

    public Set<String> get() {
        return residents;
    }
}
