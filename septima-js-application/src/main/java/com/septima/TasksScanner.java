/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima;

import com.septima.cache.ScriptDocument;
import com.septima.script.JsDoc;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author mg
 */
public class TasksScanner implements Indexer.ScanCallback {

    private final Set<String> residents = new HashSet<>();
    private final Map<String, Collection<String>> validators = new HashMap<>();

    public TasksScanner() {
        super();
    }

    @Override
    public void moduleScanned(final String aModuleName, final ScriptDocument.ModuleDocument aModuleDocument, final File aFile) {
        List<JsDoc.Tag> annotations = aModuleDocument.getAnnotations();
        if (annotations.stream()
                .anyMatch(tag -> JsDoc.Tag.RESIDENT_TAG.equalsIgnoreCase(tag.getName()))
                ) {
            residents.add(aModuleName);
        }
        annotations.stream()
                .filter(tag -> JsDoc.Tag.VALIDATOR_TAG.equalsIgnoreCase(tag.getName()))
                .findAny()
                .ifPresent((JsDoc.Tag tag) -> {
                    validators.put(aModuleName, tag.getParams());
                    Logger.getLogger(TasksScanner.class.getName()).log(Level.INFO, "Validator \"{0}\" on datasources {1} has been registered", new Object[]{aModuleName, tag.getParams().toString()});
                });
    }

    public Set<String> getResidents() {
        return residents;
    }

    public Map<String, Collection<String>> getValidators() {
        return validators;
    }
}
