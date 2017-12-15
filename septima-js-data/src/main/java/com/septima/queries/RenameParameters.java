package com.septima.queries;

import net.sf.jsqlparser.SyntaxTreeVisitor;
import net.sf.jsqlparser.expression.NamedParameter;

import java.util.Map;

public class RenameParameters extends SyntaxTreeVisitor {

    private final Map<String, String> newNames;

    RenameParameters(Map<String, String> aNewNames){
        newNames = aNewNames;
    }

    @Override
    public void visit(NamedParameter parameter) {
        parameter.setName(newNames.getOrDefault(parameter.getName(), parameter.getName()));
    }
}
