package net.sf.jsqlparser;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public abstract class SyntaxTreeVisitor implements StatementVisitor, SelectVisitor, SelectItemVisitor, OrderByVisitor, IntoTableVisitor, FromItemVisitor, ExpressionVisitor, ItemsListVisitor {

    protected void visitColumnDefinition(ColumnDefinition definition){}

    protected void visitIndex(Index index){}

    protected void visitTop(Top top){}

    protected void visitLimit(Limit limit){}

    protected void visitUnionTypes(UnionTypes unionTypes){}

    @Override
    public void visit(DoubleValue doubleValue) {
    }

    @Override
    public void visit(DateValue dateValue) {
    }

    @Override
    public void visit(LongValue longValue) {
    }

    @Override
    public void visit(NullValue nullValue) {
    }

    @Override
    public void visit(TimeValue timeValue) {
    }

    @Override
    public void visit(TimestampValue timestampValue) {
    }

    @Override
    public void visit(NamedParameter namedParameter) {
    }

    @Override
    public void visit(Drop drop) {
    }

    @Override
    public void visit(AllColumns allColumns) {
    }

    @Override
    public void visit(StringValue stringValue) {
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
    }

    protected void visitDistinct(Distinct distinct){
        if(distinct.getOnSelectItems() != null){
            distinct.getOnSelectItems().forEach(selectItem -> selectItem.accept(this));
        }
    }

    protected void visitInto(Into into){
        if(into.getTables() != null){
            into.getTables().forEach(this::visit);
        }
    }

    protected void visitAlias(Alias alias){}

    protected void visitJoin(Join join) {
        if (join.getRightItem() != null) {
            join.getRightItem().accept(this);
        }
        if (join.getUsingColumns() != null) {
            join.getUsingColumns().forEach(column -> column.accept(this));
        }
        if (join.getOnExpression() != null) {
            join.getOnExpression().accept(this);
        }
    }

    private void visitBinaryExpression(BinaryExpression binaryExpression) {
        if (binaryExpression.getLeftExpression() != null) {
            binaryExpression.getLeftExpression().accept(this);
        }
        if (binaryExpression.getRightExpression() != null) {
            binaryExpression.getRightExpression().accept(this);
        }
    }

    @Override
    public void visit(OrderByElement orderByElement){
        if(orderByElement.getExpression() != null){
            orderByElement.getExpression().accept(this);
        }
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        if(plainSelect.getDistinct() != null) {
            visitDistinct(plainSelect.getDistinct());
        }
        if(plainSelect.getSelectItems() != null){
            plainSelect.getSelectItems().forEach(selectItem -> selectItem.accept(this));
        }
        if (plainSelect.getFromItem() != null) {
            plainSelect.getFromItem().accept(this);
        }
        if (plainSelect.getJoins() != null) {
            plainSelect.getJoins().forEach(this::visitJoin);
        }
        if(plainSelect.getConnect() != null){
            plainSelect.getConnect().accept(this);
        }
        if(plainSelect.getWhere() != null){
            plainSelect.getWhere().accept(this);
        }
        if(plainSelect.getGroupByColumnReferences() != null){
            plainSelect.getGroupByColumnReferences().forEach(expression -> expression.accept(this));
        }
        if(plainSelect.getHaving() != null){
            plainSelect.getHaving().accept(this);
        }
        if(plainSelect.getInto() != null) {
            visitInto(plainSelect.getInto());
        }
        if(plainSelect.getLimit() != null){
            visitLimit(plainSelect.getLimit());
        }
        if(plainSelect.getTop() != null) {
            visitTop(plainSelect.getTop());
        }
        if(plainSelect.getOrderByElements() != null){
            plainSelect.getOrderByElements().forEach(orderByElement -> orderByElement.accept(this));
        }
    }

    @Override
    public void visit(Union union) {
        if(union.getTypeOperations() != null){
            union.getTypeOperations().forEach(this::visitUnionTypes);
        }
        if(union.getOrderByElements() != null) {
            union.getOrderByElements().forEach(orderByElement -> orderByElement.accept(this));
        }
        if(union.getLimit() != null) {
            visitLimit(union.getLimit());
        }
        if(union.getPlainSelects() != null) {
            union.getPlainSelects().forEach(plainSelect -> plainSelect.accept(this));
        }
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        if(allTableColumns.getTable() != null) {
            visit(allTableColumns.getTable());
        }
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        if(selectExpressionItem.getAlias() != null){
            visitAlias(selectExpressionItem.getAlias());
        }
        if(selectExpressionItem.getExpression() != null){
            selectExpressionItem.getExpression().accept(this);
        }
    }

    @Override
    public void visit(Table table) {
        if(table.getAlias() != null){
            visitAlias(table.getAlias());
        }
    }

    @Override
    public void visit(SubSelect subSelect) {
        if(subSelect.getAlias() != null){
            visitAlias(subSelect.getAlias());
        }
        if(subSelect.getSelectBody() != null){
            subSelect.getSelectBody().accept(this);
        }
    }

    @Override
    public void visit(Between between) {
        if (between.getLeftExpression() != null) {
            between.getLeftExpression().accept(this);
        }
        if (between.getBetweenExpressionStart() != null) {
            between.getBetweenExpressionStart().accept(this);
        }
        if (between.getBetweenExpressionEnd() != null) {
            between.getBetweenExpressionEnd().accept(this);
        }
    }

    @Override
    public void visit(Column tableColumn) {
        if (tableColumn.getTable() != null) {
            visit(tableColumn.getTable());
        }
    }

    @Override
    public void visit(Function function) {
        if (function.getParameters() != null) {
            function.getParameters().accept(this);
        }
    }

    @Override
    public void visit(InExpression inExpression) {
        if (inExpression.getLeftExpression() != null) {
            inExpression.getLeftExpression().accept(this);
        }
        if (inExpression.getItemsList() != null) {
            inExpression.getItemsList().accept(this);
        }
    }

    @Override
    public void visit(InverseExpression inverseExpression) {
        if (inverseExpression.getExpression() != null) {
            inverseExpression.getExpression().accept(this);
        }
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        if (isNullExpression.getLeftExpression() != null) {
            isNullExpression.getLeftExpression().accept(this);
        }
    }

    @Override
    public void visit(Addition addition) {
        visitBinaryExpression(addition);
    }

    @Override
    public void visit(AndExpression andExpression) {
        visitBinaryExpression(andExpression);
    }

    @Override
    public void visit(Division division) {
        visitBinaryExpression(division);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        visitBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        if (existsExpression.getRightExpression() != null) {
            existsExpression.getRightExpression().accept(this);
        }
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        visitBinaryExpression(likeExpression);
    }

    @Override
    public void visit(MinorThan minorThan) {
        visitBinaryExpression(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryExpression(minorThanEquals);
    }

    @Override
    public void visit(Multiplication multiplication) {
        visitBinaryExpression(multiplication);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(OrExpression orExpression) {
        visitBinaryExpression(orExpression);
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        if (parenthesis.getExpression() != null) {
            parenthesis.getExpression().accept(this);
        }
    }

    @Override
    public void visit(Subtraction subtraction) {
        visitBinaryExpression(subtraction);
    }

    @Override
    public void visit(ExpressionList expressionList) {
        if(expressionList.getExpressions() != null) {
            expressionList.getExpressions().forEach(expression -> expression.accept(this));
        }
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        if (caseExpression.getSwitchExpression() != null) {
            caseExpression.getSwitchExpression().accept(this);
        }
        if (caseExpression.getElseExpression() != null) {
            caseExpression.getElseExpression().accept(this);
        }
        if (caseExpression.getWhenClauses() != null) {
            caseExpression.getWhenClauses().forEach(whenClause -> whenClause.accept(this));
        }
    }

    @Override
    public void visit(WhenClause whenClause) {
        if (whenClause.getThenExpression() != null) {
            whenClause.getThenExpression().accept(this);
        }
        if (whenClause.getWhenExpression() != null) {
            whenClause.getWhenExpression().accept(this);
        }
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        if (allComparisonExpression.getSubSelect() != null) {
            visit(allComparisonExpression.getSubSelect());
        }
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        if(anyComparisonExpression.getSubSelect() != null) {
            visit(anyComparisonExpression.getSubSelect());
        }
    }

    @Override
    public void visit(SubJoin subjoin) {
        if (subjoin.getAlias() != null) {
            visitAlias(subjoin.getAlias());
        }
        if (subjoin.getLeft() != null) {
            subjoin.getLeft().accept(this);
        }
        if (subjoin.getJoin() != null) {
            visitJoin(subjoin.getJoin());
            subjoin.getJoin().getRightItem().accept(this);
        }
    }

    @Override
    public void visit(Concat concat) {
        visitBinaryExpression(concat);
    }

    @Override
    public void visit(Matches matches) {
        visitBinaryExpression(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        visitBinaryExpression(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        visitBinaryExpression(bitwiseOr);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        visitBinaryExpression(bitwiseXor);
    }

    @Override
    public void visit(Connect aConnect) {
        if (aConnect.getStartWith() != null) {
            aConnect.getStartWith().accept(this);
        }
        if (aConnect.getConnectBy() != null) {
            aConnect.getConnectBy().accept(this);
        }
    }

    public void visitWithItem(WithItem withItem) {
        if (withItem.getSelectBody() != null) {
            withItem.getSelectBody().accept(this);
        }
        if (withItem.getWithItemList() != null) {
            withItem.getWithItemList().forEach(selectItem -> selectItem.accept(this));
        }
    }

    @Override
    public void visit(Select select) {
        if (select.getSelectBody() != null) {
            select.getSelectBody().accept(this);
        }
        if (select.getWithItemsList() != null) {
            select.getWithItemsList().forEach(this::visitWithItem);
        }
    }

    @Override
    public void visit(Delete delete) {
        visit(delete.getTable());
        if (delete.getWhere() != null) {
            delete.getWhere().accept(this);
        }
    }

    @Override
    public void visit(Update update) {
        visit(update.getTable());
        if (update.getExpressions() != null) {
            update.getExpressions().forEach(expression -> expression.accept(this));
        }
        if (update.getColumns() != null) {
            update.getColumns().forEach(column -> column.accept(this));
        }
        if (update.getWhere() != null) {
            update.getWhere().accept(this);
        }
    }

    @Override
    public void visit(Insert insert) {
        visit(insert.getTable());
        if (insert.getColumns() != null) {
            insert.getColumns().forEach(column -> column.accept(this));
        }
        if (insert.getItemsList() != null) {
            insert.getItemsList().accept(this);
        }
    }

    @Override
    public void visit(Replace replace) {
        visit(replace.getTable());
        if (replace.getColumns() != null) {
            replace.getColumns().forEach(column -> column.accept(this));
        }
        if (replace.getExpressions() != null) {
            replace.getExpressions().forEach(expression -> expression.accept(this));
        }
        if (replace.getItemsList() != null) {
            replace.getItemsList().accept(this);
        }
    }

    @Override
    public void visit(Truncate truncate) {
        visit(truncate.getTable());
    }

    @Override
    public void visit(CreateTable createTable) {
        visit(createTable.getTable());
        if (createTable.getColumnDefinitions() != null) {
            createTable.getColumnDefinitions().forEach(this::visitColumnDefinition);
        }
        if (createTable.getIndexes() != null) {
            createTable.getIndexes().forEach(this::visitIndex);
        }
    }
}
