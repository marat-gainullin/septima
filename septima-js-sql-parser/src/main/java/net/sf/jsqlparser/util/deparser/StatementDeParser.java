package net.sf.jsqlparser.util.deparser;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class StatementDeParser implements StatementVisitor {

    public static String assemble(Statement syntax){
        StatementDeParser deParser = new StatementDeParser(new StringBuilder());
        syntax.accept(deParser);
        return deParser.getBuilder().toString();
    }

    protected final StringBuilder builder;

    public StatementDeParser(StringBuilder builder) {
        this.builder = builder;
    }

    public void visit(CreateTable createTable) {
        CreateTableDeParser createTableDeParser = new CreateTableDeParser(builder);
        createTableDeParser.deParse(createTable);
    }

    public void visit(Delete delete) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuilder(builder);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, builder);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        DeleteDeParser deleteDeParser = new DeleteDeParser(expressionDeParser, builder);
        deleteDeParser.deParse(delete);
    }

    public void visit(Drop drop) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuilder(builder);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, builder);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        DropDeParser dropDeParser = new DropDeParser(expressionDeParser, builder);
        dropDeParser.deParse(drop);
    }

    public void visit(Insert insert) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuilder(builder);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, builder);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        InsertDeParser insertDeParser = new InsertDeParser(expressionDeParser, selectDeParser, builder);
        insertDeParser.deParse(insert);
    }

    public void visit(Replace replace) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuilder(builder);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, builder);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        ReplaceDeParser replaceDeParser = new ReplaceDeParser(expressionDeParser, selectDeParser, builder);
        replaceDeParser.deParse(replace);
    }

    public void visit(Select select) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuilder(builder);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, builder);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
            builder.append(select.getCommentWith() != null ? select.getCommentWith() + " " : "").append(ExpressionDeParser.LINE_SEPARATOR).append("With ");

            for (int i = 0; i < select.getWithItemsList().size(); i++) {
                WithItem withItem = select.getWithItemsList().get(i);
                builder.append(withItem);
                builder.append((i < select.getWithItemsList().size() - 1) ? (!"".equals(select.getCommentsComma().get(i)) ? " " + select.getCommentsComma().get(i) + ExpressionDeParser.LINE_SEPARATOR : "") + "," : "")
                        .append(ExpressionDeParser.LINE_SEPARATOR).append(" ");
            }
        }
        select.getSelectBody().accept(selectDeParser);
        builder.append(!"".equals(select.getEndComment()) ? " " + select.getEndComment() : "");
    }

    public void visit(Truncate truncate) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuilder(builder);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, builder);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        TruncateDeParser truncateDeParser = new TruncateDeParser(expressionDeParser, builder);
        truncateDeParser.deParse(truncate);
    }

    public void visit(Update update) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuilder(builder);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, builder);
        UpdateDeParser updateDeParser = new UpdateDeParser(expressionDeParser, builder);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        updateDeParser.deParse(update);
    }

    public StringBuilder getBuilder() {
        return builder;
    }

}
