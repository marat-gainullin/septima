package com.septima.queries;

import com.septima.Entities;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.JSqlParser;
import net.sf.jsqlparser.SeptimaSqlParser;
import net.sf.jsqlparser.UncheckedJSQLParserException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.syntax.SyntaxTreeVisitor;

import java.io.StringReader;
import java.nio.file.Path;
import java.util.Map;

public class InlineEntities extends SyntaxTreeVisitor {

    private static final String APPLICATION_QUERY_REF_PREFIX = "#";

    public static void to(Statement syntax, Entities aEntities, Map<String, Map<String, String>> aParametersBinds, Path aStartOfReferences) {
        InlineEntities inline = new InlineEntities(aEntities, aParametersBinds, aStartOfReferences);
        syntax.accept(inline);
    }

    private final Entities entities;
    private final Map<String, Map<String, String>> parametersBinds;
    private final Path startOfReferences;

    private InlineEntities(Entities aQueries, Map<String, Map<String, String>> aParametersBinds, Path aStartOfReferences) {
        entities = aQueries;
        parametersBinds = aParametersBinds;
        startOfReferences = aStartOfReferences;
    }

    private FromItem fromItemToSubQuery(FromItem fromItem) throws JSQLParserException {
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            if ((table.getSchemaName() == null || table.getSchemaName().isEmpty()) &&
                    (table.getName() != null && table.getName().length() > 1 && table.getName().startsWith(APPLICATION_QUERY_REF_PREFIX))) {
                String inlinedEntityRef = table.getName().substring(1);
                String inlinedEntityName;
                if(inlinedEntityRef.startsWith("../") || inlinedEntityRef.startsWith("./")){
                    Path absoluteRef = startOfReferences.resolve(inlinedEntityRef);
                    Path entityRef = entities.getApplicationPath().relativize(absoluteRef);
                    inlinedEntityName = entityRef.toString().replace('\\', '/');
                } else {
                    inlinedEntityName = inlinedEntityRef;
                }
                SqlEntity inlinedEntity = entities.loadEntity(inlinedEntityName);
                String inlinedEntitySql = inlinedEntity.getSqlText();
                JSqlParser sqlParser = new SeptimaSqlParser();
                Statement querySyntax = sqlParser.parse(new StringReader(inlinedEntitySql));
                if (querySyntax instanceof Select) {
                    String sourceName = table.getAlias() != null && table.getAlias().getName() != null ? table.getAlias().getName() : inlinedEntityName.replaceAll("[/\\-\\.]+", "_");
                    RenameParameters rename = new RenameParameters(parametersBinds.getOrDefault(sourceName, Map.of()));
                    querySyntax.accept(rename);
                    SubSelect subSelect = new SubSelect();
                    subSelect.setSelectBody(((Select) querySyntax).getSelectBody());
                    Alias alias = new Alias();
                    alias.setName(sourceName);
                    subSelect.setAlias(alias);
                    subSelect.setCommentBeginBracket(table.getComment());
                    return subSelect;
                } else {
                    throw new IllegalStateException("Entity '" + inlinedEntityName + " can't be inlined, due to its Sql is not a 'Select' query.");
                }
            } else {
                return fromItem;
            }
        } else {
            return fromItem;
        }
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        super.visit(plainSelect);
        try {
            plainSelect.setFromItem(fromItemToSubQuery(plainSelect.getFromItem()));
        } catch (JSQLParserException ex) {
            throw new UncheckedJSQLParserException(ex);
        }
    }

    protected void visitJoin(Join join) {
        super.visitJoin(join);
        try {
            join.setRightItem(fromItemToSubQuery(join.getRightItem()));
        } catch (JSQLParserException ex) {
            throw new UncheckedJSQLParserException(ex);
        }
    }

}
