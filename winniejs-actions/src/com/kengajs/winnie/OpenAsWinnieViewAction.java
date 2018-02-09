package com.kengajs.winnie;

import com.intellij.notification.Notification;
import com.intellij.notification.NotificationType;
import com.intellij.notification.Notifications;
import com.intellij.openapi.actionSystem.AnAction;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.actionSystem.DataKeys;
import com.intellij.openapi.diagnostic.Logger;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.SystemInfo;
import com.intellij.openapi.vfs.VirtualFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OpenAsWinnieViewAction extends AnAction {

    private static final String SHELL = shellPath();
    private static final Logger LOG = Logger.getInstance(OpenAsWinnieViewAction.class);

    private static String shellPath() {
        String shell = System.getenv("SHELL");

        if (shell != null && new File(shell).canExecute()) {
            return shell;
        }

        if (SystemInfo.isUnix) {
            if (new File("/bin/bash").exists()) {
                return "/bin/bash";
            } else {
                return "/bin/sh";
            }
        } else {
            return "cmd.exe";
        }
    }

    @Override
    public void update(AnActionEvent e) {
        VirtualFile file = DataKeys.VIRTUAL_FILE.getData(e.getDataContext());
        e.getPresentation().setEnabled(file != null && !file.isDirectory() && "js".equalsIgnoreCase(file.getExtension()) && file.exists());
    }

    @Override
    public void actionPerformed(AnActionEvent e) {
        VirtualFile file = DataKeys.VIRTUAL_FILE.getData(e.getDataContext());
        Project project = DataKeys.PROJECT.getData(e.getDataContext());
        if (project != null && file != null && !file.isDirectory() && "js".equalsIgnoreCase(file.getExtension()) && file.exists()) {
            Path projectPath = Paths.get(project.getBasePath());
            Path filePath = Paths.get(file.getPath());
            Path relativeFilePath = projectPath.resolve("src/client/src").relativize(filePath);
            String relativeFileName = relativeFilePath.normalize().toString();
            String moduleId = relativeFileName.substring(0, relativeFileName.length() - 3).replace('\\', '/');
            List<String> args = new ArrayList<>(5);
            args.addAll(List.of("gradlew", "design", "-Pview=" + moduleId));
            if (SystemInfo.isWindows) {
                args.add(0, "/C");
            }
            args.add(0, SHELL);
            Notifications.Bus.notify(new Notification("winnie-actions", "Winnie open", "About to execute: " + args.stream().collect(Collectors.joining(" ")), NotificationType.INFORMATION), project);
            try {
                new ProcessBuilder(args)
                        .directory(projectPath.toFile())
                        .start()
                        .onExit()
                        .thenAccept(process -> {
                            int exitWith = process.exitValue();
                            String msg = exitWith == 0 ? "A module '" + moduleId + "' opened as Winnie view." :
                                    "Opening a module '" + moduleId + "' as Winnie view failed. Command exit code: " + exitWith;
                            Notifications.Bus.notify(new Notification("winnie-actions", "Winnie open", msg, NotificationType.INFORMATION), project);
                            LOG.info(msg);
                        });
            } catch (IOException ex) {
                Notifications.Bus.notify(new Notification("winnie-actions", "Winnie open", ex.getMessage(), NotificationType.ERROR), project);
                LOG.error(ex);
            }
        }
    }
}
