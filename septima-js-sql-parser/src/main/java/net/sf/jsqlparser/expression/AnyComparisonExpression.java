package net.sf.jsqlparser.expression;

import net.sf.jsqlparser.statement.select.SubSelect;

public class AnyComparisonExpression implements Expression {

    private SubSelect subSelect;
    private String comment;

    public AnyComparisonExpression(SubSelect subSelect) {
        this.subSelect = subSelect;
    }

    public SubSelect getSubSelect() {
        return subSelect;
    }

    @Override
    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    /**
     * @return the comment
     */
    public String getComment() {
        return comment;
    }

    /**
     * @param comment the comment transform set
     */
    public void setComment(String comment) {
        this.comment = comment;
    }
}
