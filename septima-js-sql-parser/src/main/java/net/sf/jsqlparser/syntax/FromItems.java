package net.sf.jsqlparser.syntax;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author mg
 */
public class FromItems implements SelectVisitor, FromItemVisitor, ExpressionVisitor, ItemsListVisitor {

    public enum ToCase {
        LOWER(String::toLowerCase),
        UPPER(String::toUpperCase);

        private final Function<String, String> transformer;

        ToCase(Function<String, String> aTransformer){
            transformer = aTransformer;
        }

        public String transform(String aValue){
            return transformer.apply(aValue);
        }
    }

    private final ToCase toCase;
    private final Map<String, FromItem> sources = new HashMap<>();

    private FromItems(ToCase aToCase) {
        super();
        toCase = aToCase;
    }

    public static Map<String, FromItem> find(ToCase toCase, SelectBody aSelectBody) {
        FromItems instance = new FromItems(toCase);
        aSelectBody.accept(instance);
        return Collections.unmodifiableMap(instance.sources);
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        if (plainSelect.getFromItem() != null) {
            plainSelect.getFromItem().accept(this);
        }

        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                join.getRightItem().accept(this);
            }
        }
    }

    @Override
    public void visit(Union union) {
        union.getPlainSelects().stream()
                .findFirst()
                .ifPresent(this::visit);
    }

    @Override
    public void visit(Table table) {
        String tableNameWithSchema = table.getWholeTableName();
        sources.put(toCase.transform(tableNameWithSchema), table);
        /*
         * В списке 'from' может быть указана таблица со схемой, а в списке 'select' без неё.
         * Для того, чтобы таблица из from могла быть найдена, если в её
         * имени присутствует имя схемы, её надо добавить ещё раз по имени без схемы.
         */
        if (table.getSchemaName() != null && !table.getSchemaName().isEmpty()) {
            String nameWithoutSchema = table.getName();
            sources.put(toCase.transform(nameWithoutSchema), table);
        }
        /*
         * В списке 'from' таблица может быть указана с алиасом, а её столбцы в списке 'select' как с алиасом таблицыб так и без него.
         * Для того, чтобы таблица из from могла быть найдена, её надо добавить как по имени, так и по алиасу.
         * Сначала, ьаблица добавляется по имени, а потом по алиасу для того чтобы у алиаса был приоритет в случае конфликта имен.
         */
        if (table.getAlias() != null && !table.getAlias().getName().isEmpty()) {
            String aliasName = table.getAlias().getName();
            sources.put(toCase.transform(aliasName), table);
        }
    }

    @Override
    public void visit(SubSelect subSelect) {
        sources.put(toCase.transform(subSelect.getAliasName()), subSelect);
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
    public void visit(Between between) {
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
    }

    @Override
    public void visit(Column tableColumn) {
    }

    @Override
    public void visit(Division division) {
        visitBinaryExpression(division);
    }

    @Override
    public void visit(DoubleValue doubleValue) {
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo);
    }

    @Override
    public void visit(net.sf.jsqlparser.expression.Function function) {
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
    public void visit(InExpression inExpression) {
        inExpression.getLeftExpression().accept(this);
        inExpression.getItemsList().accept(this);
    }

    @Override
    public void visit(InverseExpression inverseExpression) {
        inverseExpression.getExpression().accept(this);
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
    }

    @Override
    public void visit(NamedParameter namedParameter) {
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        visitBinaryExpression(likeExpression);
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        existsExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(LongValue longValue) {
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
    public void visit(NullValue nullValue) {
    }

    @Override
    public void visit(OrExpression orExpression) {
        visitBinaryExpression(orExpression);
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {
    }

    @Override
    public void visit(Subtraction subtraction) {
        visitBinaryExpression(subtraction);
    }

    private void visitBinaryExpression(BinaryExpression binaryExpression) {
        binaryExpression.getLeftExpression().accept(this);
        binaryExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(ExpressionList expressionList) {
        expressionList.getExpressions()
                .forEach(expression -> expression.accept(this));
    }

    @Override
    public void visit(DateValue dateValue) {
    }

    @Override
    public void visit(TimestampValue timestampValue) {
    }

    @Override
    public void visit(TimeValue timeValue) {
    }

    @Override
    public void visit(CaseExpression caseExpression) {
    }

    @Override
    public void visit(WhenClause whenClause) {
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        allComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        anyComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(SubJoin subjoin) {
        subjoin.getLeft().accept(this);
        subjoin.getJoin().getRightItem().accept(this);
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
    }
}
