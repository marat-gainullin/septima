package com.septima.application.io;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ResponseBodySender implements WriteListener {

    private final Queue<Runnable> transitions = new LinkedList<>();
    private final byte[] data;
    private final AsyncContext context;
    private final ServletOutputStream stream;

    public ResponseBodySender(byte[] aData, AsyncContext aContext, Runnable onComplete) {
        data = aData;
        context = aContext;
        try {
            stream = context.getResponse().getOutputStream();
            transitions.offer(withUIOE(() -> stream.write(data)));
            transitions.offer(withUIOE(stream::flush));
            transitions.offer(withUIOE(() -> {
                stream.close();
                onComplete.run();
            }));
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static Runnable withUIOE(WithIOException action) {
        return () -> {
            try {
                action.run();
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        };
    }

    @Override
    public void onWritePossible() {
        while (stream.isReady()) {
            Runnable action = transitions.poll();
            if (action != null) {
                action.run();
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        Logger.getLogger(ResponseBodySender.class.getName()).log(Level.SEVERE, t.getMessage(), t);
        ((HttpServletResponse) context.getResponse()).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        context.complete();
    }

    private interface WithIOException {
        void run() throws IOException;
    }
}
