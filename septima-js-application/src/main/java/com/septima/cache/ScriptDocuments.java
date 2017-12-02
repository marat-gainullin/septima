package com.septima.cache;

import com.septima.cache.ActualCacheEntry;
import com.septima.client.settings.SettingsConstants;
import com.septima.util.FileUtils;
import java.io.File;

/**
 * caches ScriptDocument by default module name for a file, i.e. app/folder/a.js will be parsed and
 * stored under key "folder/a"
 * @author mg
 */
public class ScriptDocuments extends ActualCache<ScriptDocument> {

    public ScriptDocuments() {
        super();
    }

    @Override
    public ScriptDocument get(String aDefaultModuleName, File aFile) throws Exception {
        return super.get(aDefaultModuleName, aFile);
    }

    public ScriptDocument getCachedConfig(String aDefaultModuleName) {
        ActualCacheEntry<ScriptDocument> docEntry = entries.get(aDefaultModuleName);
        return docEntry != null ? docEntry.getValue() : null;
    }

    @Override
    protected ScriptDocument parse(String aDefaultModuleName, File aFile) throws Exception {
        String source = FileUtils.readString(aFile, SettingsConstants.COMMON_ENCODING);
        return ScriptDocument.parse(source, aDefaultModuleName);
    }

}
