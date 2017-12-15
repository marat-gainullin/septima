package com.septima.queries;

import com.septima.application.ApplicationEntities;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.SyntaxTreeVisitor;
import net.sf.jsqlparser.UncheckedJSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.StringReader;
import java.util.Map;

public class InlineEntities extends SyntaxTreeVisitor {

    private static final String APPLICATION_QUERY_REF_PREFIX = "#";

    public static void to(Statement syntax, ApplicationEntities aQueries, Map<String, Map<String, String>> aParametersBinds) {
        InlineEntities inline = new InlineEntities(aQueries, aParametersBinds);
        syntax.accept(inline);
    }

    private final ApplicationEntities entities;
    private final Map<String, Map<String, String>> parametersBinds;

    private InlineEntities(ApplicationEntities aQueries, Map<String, Map<String, String>> aParametersBinds) {
        entities = aQueries;
        parametersBinds = aParametersBinds;
    }

    private FromItem fromItemToSubQuery(FromItem fromItem) throws JSQLParserException {
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            if ((table.getSchemaName() == null || table.getSchemaName().isEmpty()) &&
                    (table.getName() != null && table.getName().length() > 1 && table.getName().startsWith(APPLICATION_QUERY_REF_PREFIX))) {
                String inlinedEntityName = table.getName().substring(1);
                SqlEntity inlinedEntity = entities.loadEntity(inlinedEntityName);
                String inlinedEntitySql = inlinedEntity.getSqlText();
                CCJSqlParserManager parserManager = new CCJSqlParserManager();
                Statement querySyntax = parserManager.parse(new StringReader(inlinedEntitySql));
                if (querySyntax instanceof Select) {
                    String sourceName = table.getAlias() != null && table.getAlias().getName() != null ? table.getAlias().getName() : inlinedEntityName.replace('.', '_');
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
