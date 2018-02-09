package com.pets.design;

import com.septima.application.AsyncEndPoint;
import com.septima.application.endpoint.Answer;
import com.septima.application.exceptions.EndPointException;

import javax.servlet.annotation.WebInitParam;
import javax.servlet.annotation.WebServlet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@WebServlet(
        asyncSupported = true,
        urlPatterns = "/src/*",
        initParams = @WebInitParam(name = "base", value = "../../../../src/client/src")
)
public class SavePoint extends AsyncEndPoint {

    private Path base;
    private String encoding;

    @Override
    public void init() {
        String baseConf = getInitParameter("base");
        if (baseConf == null || baseConf.isEmpty()) {
            throw new IllegalStateException("'base' configuration parameter is missing");
        }
        base = Paths.get(getServletContext().getRealPath("/")).resolve(baseConf);
        encoding = getInitParameter("encoding");
        if (encoding == null || encoding.isEmpty()) {
            encoding = "utf-8";
        }
        super.init();
    }

    @Override
    public void post(Answer answer) {
        answer.onJsonObject()
                .thenApply(datum -> {
                    String pathInfo = answer.getRequest().getPathInfo();
                    String extension = (String) datum.get("extension");
                    String content = (String) datum.get("content");
                    if (pathInfo == null || pathInfo.isEmpty()) {
                        throw new EndPointException("Module/resource path uri part is missing");
                    }
                    try {
                        Files.write(base.resolve(pathInfo.substring(1) + (extension != null && !extension.isEmpty() ? "." + extension : "")), content.getBytes(encoding));
                        return answer;
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                })
                .thenAccept(Answer::ok);
    }
}
