package com.septima.application.exceptions;

import java.util.Set;

public class WritesNotAllowedException extends NoAccessException {

    private final static long serialVersionUID = 1L;

    public WritesNotAllowedException(String anCollectionName, Set<String> aNeededRoles) {
        super("Write access to collection '" + anCollectionName + "' data requires one of the following roles: "
                + "["
                + aNeededRoles.stream()
                .map(role -> new StringBuilder().append("'").append(role).append("'"))
                .reduce((s1, s2) -> s1.append(", ").append(s2)).orElse(new StringBuilder()).toString()
                + "]");
    }
}
