package com.kengajs.winnie.viewssource;

import com.intellij.openapi.roots.ui.configuration.CommonContentEntriesEditor;
import com.intellij.openapi.roots.ui.configuration.ModuleConfigurationState;
import org.jetbrains.jps.model.module.JpsModuleSourceRootType;

public class KengaContentEntriesEditor extends CommonContentEntriesEditor {

    public KengaContentEntriesEditor(String moduleName, ModuleConfigurationState state, JpsModuleSourceRootType<?>... rootTypes) {
        super(moduleName, state, false, rootTypes);
    }
}
