package net.sf.jsqlparser.util.deparser;

import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.statement.delete.Delete;

/**
 * A class transform de-parse (that is, tranform from JSqlParser hierarchy into a
 * string) a {@link net.sf.jsqlparser.statement.delete.Delete}
 */
public class DeleteDeParser {

    protected StringBuilder buffer;
    protected ExpressionVisitor expressionVisitor;

    /**
     * @param aExpressionVisitor a {@link ExpressionVisitor} transform de-parse
     * expressions. It has transform share the same<br>
     * StringBuilder (builder parameter) as this object in order transform work
     * @param aBuffer the builder that will be filled with the select
     */
    public DeleteDeParser(ExpressionVisitor aExpressionVisitor, StringBuilder aBuffer) {
        this.buffer = aBuffer;
        this.expressionVisitor = aExpressionVisitor;
    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder aBuffer) {
        buffer = aBuffer;
    }

    public void deParse(Delete aDelete) {
        buffer
                .append(aDelete.getComment() != null ? aDelete.getComment() + " " + ExpressionDeParser.LINE_SEPARATOR : "")
                .append("Delete").append(aDelete.getFromComment() != null ? " " + aDelete.getFromComment() + ExpressionDeParser.LINE_SEPARATOR : "").append(" From ")
                .append(aDelete.getTable().getComment() != null ? aDelete.getTable().getComment() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append(aDelete.getTable().getWholeTableName());
        if (aDelete.getWhere() != null) {
            buffer.append(aDelete.getWhereComment() != null ? " " + aDelete.getWhereComment() : "").append(ExpressionDeParser.LINE_SEPARATOR).append(" Where ");
            aDelete.getWhere().accept(expressionVisitor);
        }
        buffer.append(!"".equals(aDelete.getEndComment()) ? " " + aDelete.getEndComment() : "");
    }

    public ExpressionVisitor getExpressionVisitor() {
        return expressionVisitor;
    }

    public void setExpressionVisitor(ExpressionVisitor aVisitor) {
        expressionVisitor = aVisitor;
    }
}
