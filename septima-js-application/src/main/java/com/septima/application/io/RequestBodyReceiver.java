package com.septima.application.io;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RequestBodyReceiver implements ReadListener {

    private final byte[] buffer = new byte[1024 * 8]; // 8 Kb
    private final ByteArrayOutputStream body = new ByteArrayOutputStream(buffer.length);

    private final Consumer<byte[]> onComplete;
    private final ServletInputStream stream;

    public RequestBodyReceiver(ServletInputStream aStream, Consumer<byte[]> aOnComplete) {
        stream = aStream;
        onComplete = aOnComplete;
    }

    @Override
    public void onDataAvailable() {
        try {
            int read;
            while (stream.isReady() && (read = stream.read(buffer)) != -1) {
                body.write(buffer, 0, read);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void onAllDataRead() {
        onComplete.accept(body.toByteArray());
    }

    @Override
    public void onError(Throwable t) {
        Logger.getLogger(RequestBodyReceiver.class.getName()).log(Level.SEVERE, t.getMessage(), t);
    }
}
