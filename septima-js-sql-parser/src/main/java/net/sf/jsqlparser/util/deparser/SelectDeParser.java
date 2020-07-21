package net.sf.jsqlparser.util.deparser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Top;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.select.UnionTypes;

import java.util.Iterator;
import java.util.List;

/**
 * A class transform de-parse (that is, tranform from JSqlParser hierarchy into a
 * string) a {@link net.sf.jsqlparser.statement.select.Select}
 */
public class SelectDeParser implements SelectVisitor, OrderByVisitor, SelectItemVisitor, FromItemVisitor {

    protected StringBuilder builder;
    protected ExpressionVisitor expressionVisitor;

    public SelectDeParser() {
    }

    /**
     * @param aExpressionVisitor a {@link ExpressionVisitor} transform de-parse
     *                           expressions. It has transform share the same<br>
     *                           StringBuilder (builder parameter) as this object in order transform work
     * @param aBuilder           the builder that will be filled with the select
     */
    public SelectDeParser(ExpressionVisitor aExpressionVisitor, StringBuilder aBuilder) {
        builder = aBuilder;
        expressionVisitor = aExpressionVisitor;
    }

    public void visit(PlainSelect aPlainSelect) {
        builder.append(aPlainSelect.getComment() != null ? aPlainSelect.getComment() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("Select ");
        Top top = aPlainSelect.getTop();
        if (top != null) {
            top.toString();
        }
        if (aPlainSelect.getDistinct() != null) {
            builder.append(aPlainSelect.getDistinct().getComment() != null ? aPlainSelect.getDistinct().getComment() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("Distinct ");
            if (aPlainSelect.getDistinct().getOnSelectItems() != null) {
                builder.append(aPlainSelect.getDistinct().getCommentOn() != null ? aPlainSelect.getDistinct().getCommentOn() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("on")
                        .append(aPlainSelect.getDistinct().getCommentBeginBracket() != null ? " " + aPlainSelect.getDistinct().getCommentBeginBracket() + ExpressionDeParser.LINE_SEPARATOR : "").append(" (");
                for (int i = 0; i < aPlainSelect.getDistinct().getOnSelectItems().size(); i++) {
                    SelectItem selectItem = (SelectItem) aPlainSelect.getDistinct().getOnSelectItems().get(i);
                    selectItem.accept(this);
                    if (i < aPlainSelect.getDistinct().getOnSelectItems().size() - 1) {
                        builder.append(!"".equals(aPlainSelect.getDistinct().getCommentsComma().get(i)) ? " " + aPlainSelect.getDistinct().getCommentsComma().get(i) + ExpressionDeParser.LINE_SEPARATOR : "")
                                .append(", ");
                    }
                }
                builder.append(aPlainSelect.getDistinct().getCommentEndBracket() != null ? aPlainSelect.getDistinct().getCommentEndBracket() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append(") ");
            }

        }

        int selectItemsCounter = 0;
        for (int i = 0, s = aPlainSelect.getSelectItems().size(); i < s; i++) {
            SelectItem selectItem = (SelectItem) aPlainSelect.getSelectItems().get(i);
            selectItem.accept(this);
            if (i < aPlainSelect.getSelectItems().size() - 1) {
                builder.append(!aPlainSelect.getCommentCommaItems().get(i).toString().isEmpty() ? " " + aPlainSelect.getCommentCommaItems().get(i) + " " : "");
                if (selectItemsCounter++ == 2) {
                    selectItemsCounter = 0;
                    builder.append(ExpressionDeParser.LINE_SEPARATOR).append(", ");
                } else {
                    builder.append(", ");
                }
            }
        }
        builder.append(" ");

        if (aPlainSelect.getFromItem() != null) {
            builder.append(aPlainSelect.getCommentFrom() != null ? aPlainSelect.getCommentFrom() + " " : "")
                    .append(ExpressionDeParser.LINE_SEPARATOR).append("From ");
            aPlainSelect.getFromItem().accept(this);
        }

        if (aPlainSelect.getJoins() != null) {
            for (Iterator iter = aPlainSelect.getJoins().iterator(); iter.hasNext(); ) {
                Join join = (Join) iter.next();
                builder.append(ExpressionDeParser.LINE_SEPARATOR);
                deparseJoin(join);
            }
        }

        if (aPlainSelect.getWhere() != null) {
            builder.append(aPlainSelect.getCommentWhere() != null ? " " + aPlainSelect.getCommentWhere() : "")
                    .append(ExpressionDeParser.LINE_SEPARATOR).append(" Where ");
            aPlainSelect.getWhere().accept(expressionVisitor);
        }

        if (aPlainSelect.getConnect() != null) {
            aPlainSelect.getConnect().accept(expressionVisitor);
        }

        if (aPlainSelect.getGroupByColumnReferences() != null) {
            builder.append(aPlainSelect.getCommentGroup() != null ? " " + aPlainSelect.getCommentGroup() : "")
                    .append(ExpressionDeParser.LINE_SEPARATOR).append(" Group")
                    .append(aPlainSelect.getCommentGroupBy() != null ? " " + aPlainSelect.getCommentGroupBy() + ExpressionDeParser.LINE_SEPARATOR : "").append(" by ");
            for (int i = 0; i < aPlainSelect.getGroupByColumnReferences().size(); i++) {
                Expression columnReference = (Expression) aPlainSelect.getGroupByColumnReferences().get(i);
                columnReference.accept(expressionVisitor);
                if (i < aPlainSelect.getGroupByColumnReferences().size() - 1) {
                    builder.append(!"".equals(aPlainSelect.getCommentCommaGroupBy().get(i)) ? " " + aPlainSelect.getCommentCommaGroupBy().get(i) + ExpressionDeParser.LINE_SEPARATOR : "")
                            .append(", ");
                }
            }

        }

        if (aPlainSelect.getHaving() != null) {
            builder.append(aPlainSelect.getCommentHaving() != null ? " " + aPlainSelect.getCommentHaving() : "")
                    .append(ExpressionDeParser.LINE_SEPARATOR).append(" Having ");
            aPlainSelect.getHaving().accept(expressionVisitor);
        }

        if (aPlainSelect.getOrderByElements() != null) {
            builder.append(aPlainSelect.getCommentOrder() != null ? " " + aPlainSelect.getCommentOrder() : "")
                    .append(ExpressionDeParser.LINE_SEPARATOR).append(" Order")
                    .append(aPlainSelect.getCommentOrderBy() != null ? " " + aPlainSelect.getCommentOrderBy() + ExpressionDeParser.LINE_SEPARATOR : "").append(" by ");
            for (int i = 0; i < aPlainSelect.getOrderByElements().size(); i++) {
                OrderByElement orderByElement = (OrderByElement) aPlainSelect.getOrderByElements().get(i);
                orderByElement.accept(this);
                if (i < aPlainSelect.getOrderByElements().size() - 1) {
                    builder.append(!"".equals(aPlainSelect.getCommentCommaOrderBy().get(i)) ? " " + aPlainSelect.getCommentCommaOrderBy().get(i) + ExpressionDeParser.LINE_SEPARATOR : "")
                            .append(", ");
                }
            }
        }

        if (aPlainSelect.getLimit() != null) {
            deparseLimit(aPlainSelect.getLimit());
        }
    }

    public void visit(Union union) {
        for (int i = 0; i < union.getPlainSelects().size(); i++) {
            assert union.getTypeOperations().size() == union.getPlainSelects().size() - 1;
            if (union.getCommentsBeginBracket() != null) {
                builder.append(!union.getCommentsBeginBracket().get(i).toString().isEmpty() ? union.getCommentsBeginBracket().get(i) + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("(");
            }
            PlainSelect plainSelect = (PlainSelect) union.getPlainSelects().get(i);
            plainSelect.accept(this);
            if (union.getCommentsEndBracket() != null) {
                builder.append(!union.getCommentsEndBracket().get(i).toString().isEmpty() ? union.getCommentsEndBracket().get(i) + " " + ExpressionDeParser.LINE_SEPARATOR : "").append(")");
            }
            if (i < union.getPlainSelects().size() - 1) {
                builder.append(ExpressionDeParser.LINE_SEPARATOR).append((UnionTypes) union.getTypeOperations().get(i)).append(ExpressionDeParser.LINE_SEPARATOR);
            } else {
                builder.append(ExpressionDeParser.LINE_SEPARATOR);
            }
        }

        if (union.getOrderByElements() != null) {
            builder.append(union.getCommentOrder() != null ? " " + union.getCommentOrder() : "")
                    .append(ExpressionDeParser.LINE_SEPARATOR).append(" Order")
                    .append(union.getCommentOrderBy() != null ? " " + union.getCommentOrderBy() + ExpressionDeParser.LINE_SEPARATOR : "").append(" by ");
            for (int i = 0; i < union.getOrderByElements().size(); i++) {
                OrderByElement orderByElement = (OrderByElement) union.getOrderByElements().get(i);
                orderByElement.accept(this);
                if (i < union.getOrderByElements().size() - 1) {
                    builder.append(!"".equals(union.getCommentCommaOrderBy().get(i)) ? " " + union.getCommentCommaOrderBy().get(i) + ExpressionDeParser.LINE_SEPARATOR : "")
                            .append(", ");
                }
            }
        }

        if (union.getLimit() != null) {
            deparseLimit(union.getLimit());
        }
    }

    public void visit(OrderByElement orderBy) {
        orderBy.getExpression().accept(expressionVisitor);
        if (orderBy.isAsc()) {
            builder.append(orderBy.getComment() != null ? " " + orderBy.getComment() + ExpressionDeParser.LINE_SEPARATOR : "").append(" asc");
        }
        if (orderBy.isDesc()) {
            builder.append(orderBy.getComment() != null ? " " + orderBy.getComment() + ExpressionDeParser.LINE_SEPARATOR : "").append(" desc");
        }
    }

    public void visit(Column column) {
        builder.append(column.getComment() != null ? column.getComment() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append(column.getWholeColumnName());
    }

    public void visit(AllColumns allColumns) {
        builder.append(allColumns.getComment() != null ? allColumns.getComment() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("*");
    }

    public void visit(AllTableColumns allTableColumns) {
        builder.append(allTableColumns.getTable().getWholeTableName()).append(".*");
    }

    public void visit(SelectExpressionItem selectExpressionItem) {
        selectExpressionItem.getExpression().accept(expressionVisitor);
        if (selectExpressionItem.getAlias() != null) {
            builder.append(" ").append(selectExpressionItem.getAlias().toString());
        }

    }

    public void visit(SubSelect subSelect) {
        builder.append(subSelect.getCommentBeginBracket() != null ? subSelect.getCommentBeginBracket() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("(");
        subSelect.getSelectBody().accept(this);
        builder.append(subSelect.getCommentEndBracket() != null ? subSelect.getCommentEndBracket() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append(")");
        String alias = subSelect.getAlias() != null ? subSelect.getAlias().toString() : "";
        if (alias != null && !alias.isEmpty()) {
            builder.append(" ").append(alias);// it's very strange, but in fact oracle doesn't permit as key word if from clause
//            builder.append(" as ").append(alias);
        }
    }

    public void visit(Table aTable) {
        builder.append(aTable.getComment() != null ? aTable.getComment() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append(aTable.getWholeTableName());
        String alias = aTable.getAlias() != null ? aTable.getAlias().toString() : "";
        if (alias != null && !alias.isEmpty()) {
            builder.append(" ").append(alias);// it's very strange, but in fact oracle doesn't permit as key word if from clause
//            builder.append(" as ").append(alias);
        }
    }

    public void deparseOrderBy(List orderByElements) {
        builder.append(ExpressionDeParser.LINE_SEPARATOR).append(" Order by ");
        for (Iterator iter = orderByElements.iterator(); iter.hasNext(); ) {
            OrderByElement orderByElement = (OrderByElement) iter.next();
            orderByElement.accept(this);
            if (iter.hasNext()) {
                builder.append(", ");
            }
        }
    }

    public void deparseLimit(Limit limit) {
        // LIMIT n OFFSET skip
        if (limit.isLimitAll() || limit.getRowCountNamedParameter() != null || limit.isRowCountJdbcParameter() || limit.getRowCount() != 0) {
            builder
                    .append(ExpressionDeParser.LINE_SEPARATOR)
                    .append(limit.getCommentLimit() != null ? limit.getCommentLimit() + ExpressionDeParser.LINE_SEPARATOR : "")
                    .append(" Limit");
        }
        if (limit.isComma()) {
            builder
                    .append(limit.getCommentLimitValue() != null ? " " + limit.getCommentLimitValue() + ExpressionDeParser.LINE_SEPARATOR : "");

            builder.append(" ");
            if (limit.isOffsetJdbcParameter()) {
                builder.append("?");
            } else if (limit.getOffsetNamedParameter() != null) {
                builder.append(limit.getOffsetNamedParameter().toString());
            } else if (limit.getOffset() != 0) {
                builder.append(limit.getOffset());
            }

            builder
                    .append(limit.getCommentComma() != null ? " " + limit.getCommentComma() + ExpressionDeParser.LINE_SEPARATOR : "")
                    .append(",")
                    .append(limit.getCommentAfterCommaValue() != null ? " " + limit.getCommentAfterCommaValue() + ExpressionDeParser.LINE_SEPARATOR : "");

            builder.append(" ");
            if (limit.isRowCountJdbcParameter()) {
                builder.append("?");
            } else if (limit.getRowCountNamedParameter() != null) {
                builder.append(limit.getRowCountNamedParameter().toString());
            } else if (limit.getRowCount() != 0) {
                builder.append(limit.getRowCount());
            }
        } else {
            if (limit.isLimitAll()) {
                builder
                        .append(limit.getCommentAll() != null ? " " + limit.getCommentAll() + ExpressionDeParser.LINE_SEPARATOR : "")
                        .append(" All");
            } else if (limit.getRowCountNamedParameter() != null || limit.isRowCountJdbcParameter() || limit.getRowCount() != 0) {
                builder
                        .append(limit.getCommentLimitValue() != null ? " " + limit.getCommentLimitValue() + ExpressionDeParser.LINE_SEPARATOR : "");

                builder.append(" ");
                if (limit.isRowCountJdbcParameter()) {
                    builder.append("?");
                } else if (limit.getRowCountNamedParameter() != null) {
                    builder.append(limit.getRowCountNamedParameter().toString());
                } else if (limit.getRowCount() != 0) {
                    builder.append(limit.getRowCount());
                }
            }

            if (limit.isOffset()) {
                builder
                        .append(limit.getCommentOffset() != null ? " " + limit.getCommentOffset() + ExpressionDeParser.LINE_SEPARATOR : "")
                        .append(" Offset")
                        .append(limit.getCommentOffsetValue() != null ? " " + limit.getCommentOffsetValue() + ExpressionDeParser.LINE_SEPARATOR : "");

                builder.append(" ");
                if (limit.isOffsetJdbcParameter()) {
                    builder.append("?");
                } else if (limit.getOffsetNamedParameter() != null) {
                    builder.append(limit.getOffsetNamedParameter().toString());
                } else if (limit.getOffset() != 0) {
                    builder.append(limit.getOffset());
                }
            }
        }
    }

    public StringBuilder getBuilder() {
        return builder;
    }

    public void setBuilder(StringBuilder builder) {
        this.builder = builder;
    }

    public ExpressionVisitor getExpressionVisitor() {
        return expressionVisitor;
    }

    public void setExpressionVisitor(ExpressionVisitor visitor) {
        expressionVisitor = visitor;
    }

    public void visit(SubJoin subjoin) {
        builder.append(subjoin.getCommentBeginBracket() != null ? subjoin.getCommentBeginBracket() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("(");
        subjoin.getLeft().accept(this);
        builder.append(" ");
        deparseJoin(subjoin.getJoin());
        builder.append(subjoin.getCommentEndBracket() != null ? subjoin.getCommentEndBracket() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append(")");
    }

    public void deparseJoin(Join join) {
        if (join.isSimple()) {
            builder.append(join.getComment() != null ? join.getComment() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append(", ");
        } else {

            builder.append(" ");
            if (join.isRight()) {
                builder.append(join.getCommentRight() != null ? join.getCommentRight() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("Right ");
            } else if (join.isNatural()) {
                builder.append(join.getCommentNatural() != null ? join.getCommentNatural() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("Natural ");
            } else if (join.isFull()) {
                builder.append(join.getCommentFull() != null ? join.getCommentFull() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("Full ");
            } else if (join.isLeft()) {
                builder.append(join.getCommentLeft() != null ? join.getCommentLeft() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("Left ");
            }

            if (join.isOuter()) {
                builder.append(join.getCommentOuter() != null ? join.getCommentOuter() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("Outer ");
            } else if (join.isInner()) {
                builder.append(join.getCommentInner() != null ? join.getCommentInner() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("Inner ");
            }

            builder.append(join.getCommentJoin() != null ? join.getCommentJoin() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append("Join ");

        }

        FromItem fromItem = join.getRightItem();
        fromItem.accept(this);
        if (join.getOnExpression() != null) {
            builder.append(join.getCommentOn() != null ? " " + join.getCommentOn() + ExpressionDeParser.LINE_SEPARATOR : "").append(" on ");
            join.getOnExpression().accept(expressionVisitor);
        }
        if (join.getUsingColumns() != null) {
            builder.append(join.getCommentUsing() != null ? " " + join.getCommentUsing() + ExpressionDeParser.LINE_SEPARATOR : "").append(" Using")
                    .append(join.getCommentBeginBracket() != null ? " " + join.getCommentBeginBracket() + ExpressionDeParser.LINE_SEPARATOR : "").append(" ( ");
            for (int i = 0, s = join.getUsingColumns().size(); i < s; i++) {
                Column column = (Column) join.getUsingColumns().get(i);
                builder.append(column.getComment() != null ? column.getComment() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append(column.getWholeColumnName());
                if (i < join.getUsingColumns().size() - 1) {
                    builder.append(!join.getCommentComma().get(i).toString().isEmpty() ? " " + join.getCommentComma().get(i) + " " + ExpressionDeParser.LINE_SEPARATOR : "");
                    builder.append(", ");
                }
            }
            builder.append(join.getCommentEndBracket() != null ? join.getCommentEndBracket() + " " + ExpressionDeParser.LINE_SEPARATOR : "").append(")");
        }
    }
}
