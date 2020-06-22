package com.septima.application.endpoint;

import com.septima.application.AsyncEndPoint;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

public class LoggedInEndPoint extends AsyncEndPoint {

    @Override
    public void get(Answer answer) {
        Principal principal = answer.getRequest().getUserPrincipal();
        Map<String, Object> body = new HashMap<>();
        body.put("userName", principal != null ? principal.getName() : null);
        answer.withJsonObject(body);
    }
}
