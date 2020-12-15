package com.kengajs.winnie.viewssource;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.jps.model.ex.JpsElementBase;

public class ViewsSourceRootProperties extends JpsElementBase<ViewsSourceRootProperties> {

    @Override
    public @NotNull ViewsSourceRootProperties createCopy() {
        return new ViewsSourceRootProperties();
    }

    @Override
    public void applyChanges(@NotNull ViewsSourceRootProperties viewsSourceRootProperties) {
    }
}
