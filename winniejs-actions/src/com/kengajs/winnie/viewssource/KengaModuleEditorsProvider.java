package com.kengajs.winnie.viewssource;

import com.intellij.openapi.module.ModuleConfigurationEditor;
import com.intellij.openapi.roots.ModifiableRootModel;
import com.intellij.openapi.roots.ui.configuration.*;
import com.intellij.openapi.module.Module;

public class KengaModuleEditorsProvider implements ModuleConfigurationEditorProvider {

    public ModuleConfigurationEditor[] createEditors(ModuleConfigurationState state) {
        ModifiableRootModel rootModel = state.getRootModel();
        Module module = rootModel.getModule();
        String moduleName = module.getName();
        return new ModuleConfigurationEditor[]{new KengaContentEntriesEditor(moduleName, state, ViewsSourceRootType.INSTANCE)};
    }
}
