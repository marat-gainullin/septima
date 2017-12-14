package com.septima.queries;

import net.sf.jsqlparser.SyntaxTreeVisitor;
import net.sf.jsqlparser.expression.NamedParameter;

import java.util.Map;

public class ParametersRename extends SyntaxTreeVisitor {

    private final Map<String, String> newNames;

    public ParametersRename(Map<String, String> aNewNames){
        newNames = aNewNames;
    }

    @Override
    public void visit(NamedParameter parameter) {
        parameter.setName(newNames.getOrDefault(parameter.getName(), parameter.getName()));
    }
}
