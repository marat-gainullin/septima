package com.septima.application.endpoint;

import com.septima.application.AsyncEndPoint;

import java.security.Principal;
import java.util.Map;

public class LoggedInEndPoint extends AsyncEndPoint {

    @Override
    public void get(Answer answer) {
        Principal principal = answer.getRequest().getUserPrincipal();
        answer.withJsonObject(Map.of("userName", principal != null ? principal.getName() : ""));
    }
}
