package com.septima.queries;

import com.septima.ApplicationEntities;
import net.sf.jsqlparser.SyntaxTreeVisitor;
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

    private FromItem fromItemToSubQuery(FromItem fromItem) {
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            if ((table.getSchemaName() == null || table.getSchemaName().isEmpty()) &&
                    (table.getName() != null && table.getName().length() > 1 && table.getName().startsWith(APPLICATION_QUERY_REF_PREFIX))) {
                try {
                    String inlinedEntityName = table.getName().substring(1);
                    SqlQuery inlinedEntity = entities.loadEntity(inlinedEntityName);
                    String inlinedEntitySql = inlinedEntity.getSqlText();
                    CCJSqlParserManager parserManager = new CCJSqlParserManager();
                    Statement querySyntax = parserManager.parse(new StringReader(inlinedEntitySql));
                    if (querySyntax instanceof Select) {
                        String sourceName = table.getAlias() != null && table.getAlias().getName() != null ? table.getAlias().getName() : inlinedEntityName.replace('.', '_');
                        ParametersRename rename = new ParametersRename(parametersBinds.getOrDefault(sourceName, Map.of()));
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
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
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
        plainSelect.setFromItem(fromItemToSubQuery(plainSelect.getFromItem()));
    }

    protected void visitJoin(Join join) {
        super.visitJoin(join);
        join.setRightItem(fromItemToSubQuery(join.getRightItem()));
    }

}
