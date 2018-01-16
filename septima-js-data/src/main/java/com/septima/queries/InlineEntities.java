package com.septima.queries;

import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import net.sf.jsqlparser.JSqlParser;
import net.sf.jsqlparser.JSqlParserException;
import net.sf.jsqlparser.SeptimaSqlParser;
import net.sf.jsqlparser.UncheckedJSqlParserException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.syntax.SyntaxTreeVisitor;

import java.io.StringReader;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public class InlineEntities extends SyntaxTreeVisitor {

    private static final String HASH = "#";

    public static void to(Statement syntax, SqlEntities aEntities, Map<String, Map<String, String>> aParametersBinds, Path aStartOfReferences, Set<String> aIllegalReferences) {
        InlineEntities inline = new InlineEntities(aEntities, aParametersBinds, aStartOfReferences, aIllegalReferences);
        syntax.accept(inline);
    }

    private final SqlEntities entities;
    private final Map<String, Map<String, String>> parametersBinds;
    private final Path startOfReferences;
    private final Set<String> illegalReferences;

    private InlineEntities(SqlEntities aQueries, Map<String, Map<String, String>> aParametersBinds, Path aStartOfReferences, Set<String> aIllegalReferences) {
        entities = aQueries;
        parametersBinds = aParametersBinds;
        startOfReferences = aStartOfReferences;
        illegalReferences = aIllegalReferences;
    }

    private FromItem fromItemToSubEntity(FromItem fromItem) {
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            if ((table.getSchemaName() == null || table.getSchemaName().isEmpty()) &&
                    (table.getName() != null && table.getName().length() > 1 && table.getName().startsWith(HASH))) {
                String inlinedEntityRef = table.getName().substring(1);
                String inlinedEntityName;
                if (inlinedEntityRef.startsWith("../") || inlinedEntityRef.startsWith("./")) {
                    Path absoluteRef = startOfReferences.resolve(inlinedEntityRef);
                    Path entityRef = entities.getEntitiesRoot().relativize(absoluteRef);
                    inlinedEntityName = entityRef.normalize().toString().replace('\\', '/');
                } else {
                    inlinedEntityName = inlinedEntityRef;
                }
                SqlEntity inlinedEntity = entities.loadEntity(inlinedEntityName, illegalReferences);
                String inlinedEntitySql = inlinedEntity.getSqlText();
                JSqlParser sqlParser = new SeptimaSqlParser();
                try {
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
                        throw new IllegalStateException("Entity '" + inlinedEntityName + " can't be inlined, due transform its Sql is not a 'Select' query.");
                    }
                } catch (JSqlParserException ex) {
                    throw new UncheckedJSqlParserException(ex);
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
        plainSelect.setFromItem(fromItemToSubEntity(plainSelect.getFromItem()));
    }

    protected void visitJoin(Join join) {
        super.visitJoin(join);
        join.setRightItem(fromItemToSubEntity(join.getRightItem()));
    }

}
