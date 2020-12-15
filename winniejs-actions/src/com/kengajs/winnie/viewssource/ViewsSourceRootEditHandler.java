package com.kengajs.winnie.viewssource;

import com.intellij.DynamicBundle;
import com.intellij.openapi.actionSystem.CustomShortcutSet;
import com.intellij.openapi.roots.ui.configuration.ModuleSourceRootEditHandler;
import com.intellij.openapi.util.IconLoader;
import com.intellij.ui.JBColor;
import org.jetbrains.annotations.Nls;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.swing.*;
import java.awt.*;
import java.util.ResourceBundle;

public class ViewsSourceRootEditHandler extends ModuleSourceRootEditHandler<ViewsSourceRootProperties> {

    private static final String ROOT_TYPE_NAME = "bearsoft.tools.winnie.rootTypeName.title";
    private static final String ROOTS_GROUP_NAME = "bearsoft.tools.winnie.rootsGroupName.title";
    private static final String UNMARK_ROOT_NAME = "bearsoft.tools.winnie.unmarkButton.title";

    private static final ResourceBundle BUNDLE = DynamicBundle.INSTANCE.getResourceBundle("i18n.bundle", ViewsSourceRootEditHandler.class.getClassLoader());
    private static final Icon VIEWS_SOURCE_ROOT_ICON = IconLoader.getIcon("views-source-root.svg", ViewsSourceRootEditHandler.class);

    protected ViewsSourceRootEditHandler() {
        super(ViewsSourceRootType.INSTANCE);
    }

    @Override
    public @NotNull
    @Nls(capitalization = Nls.Capitalization.Title)
    String getRootTypeName() {
        return BUNDLE.getString(ROOT_TYPE_NAME);
    }

    @Override
    public @NotNull
    Icon getRootIcon() {
        return VIEWS_SOURCE_ROOT_ICON;
    }

    @Override
    public @Nullable
    Icon getFolderUnderRootIcon() {
        return null;
    }

    @Override
    public @Nullable
    CustomShortcutSet getMarkRootShortcutSet() {
        return null;
    }

    @Override
    public @NotNull
    @Nls(capitalization = Nls.Capitalization.Title)
    String getRootsGroupTitle() {
        return BUNDLE.getString(ROOTS_GROUP_NAME);
    }

    @Override
    public @NotNull
    Color getRootsGroupColor() {
        return new JBColor(new Color(0xF3F02D), new Color(193, 158, 90));
    }

    @Override
    public @NotNull
    @Nls(capitalization = Nls.Capitalization.Title)
    String getUnmarkRootButtonText() {
        return BUNDLE.getString(UNMARK_ROOT_NAME);
    }
}
