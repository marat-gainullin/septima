package com.septima.queries;

import com.septima.metadata.Parameter;
import net.sf.jsqlparser.expression.NamedParameter;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.syntax.SyntaxTreeVisitor;

import java.util.LinkedHashMap;
import java.util.Map;

public class ExtractParameters extends SyntaxTreeVisitor {

    private final Map<String, Parameter> extracted = new CaseInsensitiveMap<>(new LinkedHashMap<>());

    private ExtractParameters() {
    }

    @Override
    public void visit(NamedParameter parameter) {
        extracted.put(parameter.getName(), new Parameter(parameter.getName()));
    }

    public static Map<String, Parameter> from(Statement aSyntax) {
        ExtractParameters extractor = new ExtractParameters();
        aSyntax.accept(extractor);
        return extractor.extracted;
    }
}
