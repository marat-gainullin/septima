package com.septima.application.endpoint;

import com.septima.application.AsyncEndPoint;
import com.septima.application.exceptions.InvalidRequestException;

import javax.servlet.http.HttpSession;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KeepMeEndPoint extends AsyncEndPoint {

    private static final int SHORT_SESSION_KEEP_FOR_DEFAULT = 30 * 60;
    private static final int LONG_SESSION_KEEP_FOR_DEFAULT = 60 * 60 * 24 * 7; // 7 days

    private volatile int longSessionKeepFor = SHORT_SESSION_KEEP_FOR_DEFAULT;

    @Override
    protected void prepare() {
        try {
            longSessionKeepFor = Integer.parseInt(getServletContext().getInitParameter("long.session.keep.for"));
        } catch (NumberFormatException ex) {
            longSessionKeepFor = LONG_SESSION_KEEP_FOR_DEFAULT;
            Logger.getLogger(KeepMeEndPoint.class.getName()).log(Level.WARNING, "Unable to read context parameter 'long.session.keep.for'. Default of 7 days will be used");
        }
    }

    @Override
    public void destroy() {
        longSessionKeepFor = SHORT_SESSION_KEEP_FOR_DEFAULT;
    }

    @Override
    public void get(Answer answer) {
        HttpSession session = answer.getRequest().getSession(false);
        if (session != null && answer.getRequest().getUserPrincipal() != null) {
            session.setMaxInactiveInterval(longSessionKeepFor);
            Arrays.stream(answer.getRequest().getCookies())
                    .filter(cookie -> "JSESSIONID".equalsIgnoreCase(cookie.getName()))
                    .findFirst()
                    .ifPresentOrElse(cookie -> answer.getResponse().setHeader("Set-Cookie", cookie.getName() + "=" + cookie.getValue() +
                                    (!session.getServletContext().getContextPath().isEmpty() ? "; Path=" + session.getServletContext().getContextPath() : "/") +
                                    "; Expires=" + DateTimeFormatter.RFC_1123_DATE_TIME.format(Instant.now().plusSeconds(longSessionKeepFor).atZone(ZoneId.of("GMT"))) +
                                    "; HttpOnly" +
                                    (answer.getRequest().isSecure() ? "; Secure" : "")
                            ),
                            () -> {
                                throw new InvalidRequestException("Only cookie based sessions are supported");
                            });
            answer.ok();
        } else {
            answer.exceptionally(new InvalidRequestException("Only authenticated users can request long live session"));
        }
    }
}
