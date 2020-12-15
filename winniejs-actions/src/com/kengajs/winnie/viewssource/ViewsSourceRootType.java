package com.kengajs.winnie.viewssource;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.jps.model.ex.JpsElementTypeBase;
import org.jetbrains.jps.model.module.JpsModuleSourceRootType;

public final class ViewsSourceRootType extends JpsElementTypeBase<ViewsSourceRootProperties> implements JpsModuleSourceRootType<ViewsSourceRootProperties> {

    public static final ViewsSourceRootType INSTANCE = new ViewsSourceRootType();

    private ViewsSourceRootType() {
    }

    @Override
    public @NotNull
    ViewsSourceRootProperties createDefaultProperties() {
        return new ViewsSourceRootProperties();
    }

    @Override
    public boolean isForTests() {
        return false;
    }
}
